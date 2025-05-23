use crate::types::{Collector, Item};
use crate::{
    DbPoolSnafu, DbPrepareSnafu, DbReadSnafu, DbWriteSnafu, Error, NetworkSnafu, PageSnafu,
    SerializationSnafu,
};
use fallible_iterator::FallibleIterator;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use reqwest::{Client, StatusCode};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::json;
use snafu::{OptionExt, ResultExt};
use soup::{NodeExt, QueryBuilderExt, Soup};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::task::spawn_blocking;
use tokio::time::{interval, sleep, MissedTickBehavior};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct CollectionResult {
    pub items: Vec<Item>,
    pub more_available: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct CollectionData {
    pub last_token: Option<String>,
    pub item_count: i64,
    pub batch_size: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ItemCache {
    pub collection: HashMap<String, Item>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct InitialResult {
    pub fan_data: Collector,
    pub collection_data: CollectionData,
    pub item_cache: ItemCache,
}
const SELECT_PRESENT_AND_RECENT_COLLECTOR: &str = r#"
select unixepoch('now') - unixepoch(last_updated, '30 days') from collector where username = ?
"#;

fn collector_present_and_recent(db: &Connection, name: &str) -> Result<bool, Error> {
    let mut stmt = db
        .prepare_cached(SELECT_PRESENT_AND_RECENT_COLLECTOR)
        .context(DbPrepareSnafu)?;
    let present = stmt
        .query([name])
        .context(DbReadSnafu)?
        .next()
        .context(DbReadSnafu)?
        .and_then(|row| row.get::<usize, i64>(0).ok())
        .map(|v| v < 0)
        .unwrap_or(false); // not present
    Ok(present)
}

const INSERT_COLLECTOR: &str = r#"
insert into collector (fan_id, username, name, token, last_updated)
values (?, ?, ?, ?, 0)
on conflict do update set token = case when token is null then excluded.token else token end"#;

pub fn add_collector(db: &Connection, collector: &Collector) -> Result<(), Error> {
    let mut stmt = db
        .prepare_cached(INSERT_COLLECTOR)
        .context(DbPrepareSnafu)?;
    stmt.execute((
        collector.fan_id,
        &collector.username,
        &collector.name,
        &collector.token,
    ))
    .context(DbWriteSnafu)?;
    Ok(())
}

const INSERT_ITEM: &str = r#"
insert into item (
    item_id, item_type, item_title, item_url, band_id, band_name, token,
    also_collected_count, last_updated
) values (?, ?, ?, ?, ?, ?, ?, ?, 0)
on conflict do update set token = case when token is null then excluded.token else token end"#;

const INSERT_COLLECTS: &str = r#"
insert or ignore into collects (fan_id, item_id)
values (?, ?)
returning 1"#;

fn add_item_for_collector(db: &Connection, fan_id: i64, item: &Item) -> Result<bool, Error> {
    let item_id = item.album_id.unwrap_or(item.item_id);
    let mut stmt = db.prepare_cached(INSERT_ITEM).context(DbPrepareSnafu)?;
    stmt.execute((
        item_id,
        &item.item_type,
        item.album_title.as_ref().unwrap_or(&item.item_title),
        &item.item_url,
        item.band_id,
        &item.band_name,
        &item.token,
        item.also_collected_count,
    ))
    .context(DbWriteSnafu)?;
    // query returns value if not present
    let mut stmt = db.prepare_cached(INSERT_COLLECTS).context(DbPrepareSnafu)?;
    let res = stmt
        .query((fan_id, item_id))
        .context(DbWriteSnafu)?
        .next()
        .context(DbReadSnafu)?
        .is_none();
    Ok(res)
}

struct InitialPage {
    fan_id: i64,
    last_token: Option<String>,
}

async fn get_initial_page(
    db: &Pool<SqliteConnectionManager>,
    name: &str,
) -> Result<InitialPage, Error> {
    println!("Reading initial page for {name}");
    let client = Client::new();
    let page = client
        .get(format!("https://bandcamp.com/{name}"))
        .send()
        .await
        .context(NetworkSnafu)?;
    if page.status() == StatusCode::TOO_MANY_REQUESTS {
        return Err(Error::RateLimit);
    }
    if page.status() == StatusCode::NOT_FOUND {
        return Err(Error::NotFoundError);
    }
    let body = page.text().await.context(NetworkSnafu)?;
    let db = db.clone();
    spawn_blocking(move || {
        let soup = Soup::new(&body);
        let node = soup.attr("id", "pagedata").find().context(PageSnafu)?;
        let attrs = node.attrs();
        let body = attrs.get("data-blob").context(PageSnafu)?;
        let result: InitialResult = serde_json::from_str(body).context(SerializationSnafu)?;
        let conn = db.get().context(DbPoolSnafu)?;
        add_collector(&conn, &result.fan_data)?;
        let mut done = false;
        for entry in result.item_cache.collection.into_values() {
            done = add_item_for_collector(&conn, result.fan_data.fan_id, &entry)?;
        }
        let more_available =
            !done && result.collection_data.item_count > result.collection_data.batch_size;
        Ok(InitialPage {
            fan_id: result.fan_data.fan_id,
            last_token: if more_available {
                result.collection_data.last_token
            } else {
                None
            },
        })
    })
    .await
    .unwrap()
}

async fn get_next_page(
    db: &Pool<SqliteConnectionManager>,
    fan_id: i64,
    mut last_token: String,
) -> Result<Option<String>, Error> {
    let client = Client::new();
    let result = client
        .post("https://bandcamp.com/api/fancollection/1/collection_items")
        .body(
            json!({
                "count": 500,
                "fan_id": fan_id,
                "older_than_token": last_token,
            })
            .to_string(),
        )
        .send()
        .await
        .context(NetworkSnafu)?;
    if result.status() == StatusCode::TOO_MANY_REQUESTS {
        return Err(Error::RateLimit);
    }
    let body = result.text().await.context(NetworkSnafu)?;
    let db = db.clone();
    spawn_blocking(move || {
        let collection_result: CollectionResult =
            serde_json::from_str(&body).context(SerializationSnafu)?;
        let mut done = false;
        let conn = db.get().context(DbPoolSnafu)?;
        for entry in collection_result.items {
            done = add_item_for_collector(&conn, fan_id, &entry)?;
            last_token = entry.token.unwrap();
        }
        if done || !collection_result.more_available {
            Ok(None)
        } else {
            Ok(Some(last_token))
        }
    })
    .await
    .unwrap()
}

pub async fn fetch_collection(
    db: &Pool<SqliteConnectionManager>,
    name: &str,
    force: bool,
) -> Result<(), Error> {
    let conn = db.get().context(DbPoolSnafu)?;
    if !force && collector_present_and_recent(&conn, name)? {
        return Ok(());
    }
    drop(conn);
    let result = get_initial_page(db, name).await?;
    if let Some(mut last_token) = result.last_token {
        println!("Reading next page for {name}");
        while let Some(token) = get_next_page(db, result.fan_id, last_token).await? {
            println!("Reading next page for {name}");
            last_token = token
        }
    }
    Ok(())
}

const SELECT_COLLECTION_SIZE: &str = r#"
select count(*) from collector
join collects using (fan_id)
where username = ?"#;

pub fn get_collection_size(db: &Pool<SqliteConnectionManager>, name: &str) -> Result<u64, Error> {
    let conn = db.get().context(DbPoolSnafu)?;
    let mut stmt = conn
        .prepare_cached(SELECT_COLLECTION_SIZE)
        .context(DbPrepareSnafu)?;
    let result = stmt
        .query([name])
        .context(DbReadSnafu)?
        .map(|r| r.get(0))
        .next()
        .context(DbReadSnafu)?;
    Ok(result.unwrap_or(0))
}

const SELECT_FIRST_QUEUE_COLLECTOR: &str = r#"
select username from collector_collection_queue
join collector using (fan_id)
order by fan_id asc
limit 1"#;

const SELECT_UNFINISHED: &str = r#"
select username from collector
where unixepoch('now') > unixepoch(last_updated, '30 days')
order by fan_id asc
limit 1"#;

fn get_next_collector(db: &Connection, crawl: bool) -> Result<Option<String>, Error> {
    let mut stmt = db
        .prepare_cached(SELECT_FIRST_QUEUE_COLLECTOR)
        .context(DbPrepareSnafu)?;
    let mut rows = stmt.query([]).context(DbReadSnafu)?;
    let row = rows.next().context(DbReadSnafu)?;
    if let Some(row) = row {
        let username: String = row.get("username").context(DbReadSnafu)?;
        Ok(Some(username))
    } else if crawl {
        let mut stmt = db
            .prepare_cached(SELECT_UNFINISHED)
            .context(DbPrepareSnafu)?;
        let mut rows = stmt.query([]).context(DbReadSnafu)?;
        let row = rows.next().context(DbReadSnafu)?;
        if let Some(row) = row {
            let username: String = row.get("username").context(DbReadSnafu)?;
            Ok(Some(username))
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

const MARK_COLLECTOR_DONE: &str = r#"
update collector
set last_updated = unixepoch('now')
where username = ?"#;

fn mark_collector_done(db: &Connection, name: &str) -> Result<(), Error> {
    let mut stmt = db
        .prepare_cached(MARK_COLLECTOR_DONE)
        .context(DbPrepareSnafu)?;
    stmt.execute([name]).context(DbWriteSnafu)?;
    Ok(())
}

const DELETE_QUEUE_COLLECTOR: &str = r#"
delete from collector_collection_queue where fan_id = (
select fan_id from collector where username = ?
)"#;

fn remove_from_queue(db: &Connection, collector: &str) -> Result<(), Error> {
    let mut stmt = db
        .prepare_cached(DELETE_QUEUE_COLLECTOR)
        .context(DbPrepareSnafu)?;
    stmt.execute([collector]).context(DbWriteSnafu)?;
    Ok(())
}

const DELETE_COLLECTS: &str = r#"
delete from collects where fan_id = (
select fan_id from collector where username = ?
)"#;

fn remove_collects(db: &Connection, name: &str) -> Result<(), Error> {
    let mut stmt = db.prepare_cached(DELETE_COLLECTS).context(DbPrepareSnafu)?;
    stmt.execute([name]).context(DbWriteSnafu)?;
    Ok(())
}

pub async fn collection_worker(
    db: &Pool<SqliteConnectionManager>,
    crawl: bool,
    run_state: &AtomicBool,
) -> Result<(), Error> {
    let mut timer = interval(Duration::from_secs(3));
    timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
    while run_state.load(Ordering::Relaxed) {
        let conn = db.get().context(DbPoolSnafu)?;
        if let Some(collector) = get_next_collector(&conn, crawl)? {
            drop(conn);
            match fetch_collection(db, &collector, false).await {
                Err(Error::RateLimit) => {
                    println!("Rate limited, waiting 10 seconds");
                    let conn = db.get().context(DbPoolSnafu)?;
                    remove_collects(&conn, &collector)?;
                    sleep(Duration::from_secs(10)).await
                }
                Err(Error::NotFoundError) => {
                    println!("Collector {collector} not found");
                    let conn = db.get().context(DbPoolSnafu)?;
                    mark_collector_done(&conn, &collector)?;
                    remove_from_queue(&conn, &collector)?;
                }
                Err(err) => {
                    println!("Error while processing collector {collector}: {err}");
                }
                Ok(()) => {
                    let conn = db.get().context(DbPoolSnafu)?;
                    mark_collector_done(&conn, &collector)?;
                    remove_from_queue(&conn, &collector)?;
                }
            }
        }
        timer.tick().await;
    }
    Ok(())
}

const SELECT_FAN_ID_FOR_NAME: &str = r#"
select fan_id from collector where username = ?
"#;

pub fn get_fan_id_for_username(db: &Connection, name: &str) -> Result<Option<i64>, Error> {
    let mut stmt = db
        .prepare_cached(SELECT_FAN_ID_FOR_NAME)
        .context(DbPrepareSnafu)?;
    #[allow(clippy::let_and_return)]
    let result = stmt
        .query([name])
        .context(DbReadSnafu)?
        .next()
        .context(DbReadSnafu)?
        .map(|r| r.get(0))
        .transpose()
        .context(DbReadSnafu);
    result
}
