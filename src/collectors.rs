use crate::types::{Collector, Item};
use crate::{
    DbPrepareSnafu, DbReadSnafu, DbWriteSnafu, Error, NetworkSnafu, PageSnafu, SerializationSnafu,
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
    pub last_token: String,
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
    more_available: bool,
    fan_id: i64,
    last_token: String,
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
        let conn = db.get().unwrap();
        add_collector(&conn, &result.fan_data)?;
        let mut done = false;
        for entry in result.item_cache.collection.into_values() {
            done = add_item_for_collector(&conn, result.fan_data.fan_id, &entry)?;
        }
        Ok(InitialPage {
            more_available: !done
                && result.collection_data.item_count > result.collection_data.batch_size,
            fan_id: result.fan_data.fan_id,
            last_token: result.collection_data.last_token,
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
        let conn = db.get().unwrap();
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
    if !force && collector_present_and_recent(&db.get().unwrap(), name)? {
        return Ok(());
    }
    let result = get_initial_page(db, name).await?;
    if result.more_available {
        let mut last_token = result.last_token;
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
    let conn = db.get().unwrap();
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
select fan_id, username from collector_collection_queue
join collector using (fan_id) limit 1"#;

const DELETE_QUEUE_COLLECTOR: &str = r#"
delete from collector_collection_queue where fan_id = ?"#;

fn get_next_collector(db: &Connection) -> Result<Option<String>, Error> {
    let mut stmt = db
        .prepare_cached(SELECT_FIRST_QUEUE_COLLECTOR)
        .context(DbPrepareSnafu)?;
    let mut rows = stmt.query([]).context(DbReadSnafu)?;
    let row = rows.next().context(DbReadSnafu)?;
    if let Some(row) = row {
        let fan_id: i64 = row.get("fan_id").context(DbReadSnafu)?;
        let username: String = row.get("username").context(DbReadSnafu)?;
        let mut stmt = db
            .prepare_cached(DELETE_QUEUE_COLLECTOR)
            .context(DbPrepareSnafu)?;
        stmt.execute([fan_id]).context(DbWriteSnafu)?;
        Ok(Some(username))
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

const ADD_COLLECTOR_TO_QUEUE: &str = r#"
insert into collector_collection_queue
values ((select fan_id from collector where username = ?))"#;

fn add_collector_to_queue(db: &Connection, collector: &str) -> Result<(), Error> {
    let mut stmt = db
        .prepare_cached(ADD_COLLECTOR_TO_QUEUE)
        .context(DbPrepareSnafu)?;
    stmt.execute([collector]).context(DbWriteSnafu)?;
    Ok(())
}

pub async fn collection_worker(db: &Pool<SqliteConnectionManager>) -> Result<(), Error> {
    let mut timer = interval(Duration::from_secs(1));
    timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        if let Some(collector) = get_next_collector(&db.get().unwrap())? {
            match fetch_collection(db, &collector, false).await {
                Err(Error::RateLimit) => {
                    println!("Rate limited, waiting 10 seconds");
                    // Need to re-add collector as it was removed from the queue by get_next_collector
                    add_collector_to_queue(&db.get().unwrap(), &collector)?;
                    sleep(Duration::from_secs(10)).await
                }
                Err(Error::NotFoundError) => {
                    println!("Collector {collector} not found");
                    mark_collector_done(&db.get().unwrap(), &collector)?;
                }
                Err(err) => {
                    add_collector_to_queue(&db.get().unwrap(), &collector)?;
                    println!("Error while processing collector {collector}: {err}");
                }
                Ok(()) => {
                    mark_collector_done(&db.get().unwrap(), &collector)?;
                }
            }
        }
        timer.tick().await;
    }
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
