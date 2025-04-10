use crate::collectors::add_collector;
use crate::types::{Collector, Item};
use crate::{
    DbPoolSnafu, DbPrepareSnafu, DbReadSnafu, DbResultSnafu, DbWriteSnafu, Error, NetworkSnafu,
    PageSnafu, SerializationSnafu,
};
use lazy_static::lazy_static;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use regex::Regex;
use reqwest::{Client, StatusCode};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::json;
use snafu::{OptionExt, ResultExt};
use soup::{NodeExt, QueryBuilderExt, Soup};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::task::spawn_blocking;
use tokio::time::{interval, sleep, MissedTickBehavior};

#[derive(Serialize, Deserialize)]
pub struct CollectorsData {
    pub thumbs: Vec<Collector>,
    pub more_thumbs_available: bool,
    pub shown_thumbs: Vec<Collector>,
}

#[derive(Serialize, Deserialize)]
pub struct AlbumProperties {
    pub item_type: String,
    pub item_id: i64,
}

#[derive(Serialize, Deserialize)]
pub struct CollectorsResult {
    pub results: Vec<Collector>,
    pub more_available: bool,
}

const SELECT_PRESENT_AND_RECENT_ITEM: &str = r#"
select unixepoch('now') - unixepoch(last_updated, '30 days') from item where item_id = ?
"#;

fn item_present_and_recent(db: &Connection, item_id: i64) -> Result<bool, Error> {
    let mut stmt = db
        .prepare_cached(SELECT_PRESENT_AND_RECENT_ITEM)
        .context(DbPrepareSnafu)?;
    let present = stmt
        .query([item_id])
        .context(DbReadSnafu)?
        .next()
        .context(DbReadSnafu)?
        .and_then(|row| row.get::<usize, i64>(0).ok())
        .map(|v| v < 0)
        .unwrap_or(false); // not present
    Ok(present)
}

const SELECT_ITEM: &str = r#"
select *
from item where item_id = ?"#;

pub fn get_item(db: &Connection, track_id: i64) -> Result<Item, Error> {
    let mut stmt = db.prepare_cached(SELECT_ITEM).context(DbPrepareSnafu)?;
    let mut rows = stmt.query([track_id]).context(DbReadSnafu)?;
    let res = rows.next().context(DbReadSnafu)?.context(DbResultSnafu)?;
    crate::types::item_from_row(res).context(DbReadSnafu)
}

const INSERT_COLLECTED_BY: &str = r#"
insert or ignore into collected_by (item_id, fan_id)
values (?, ?)
returning 1"#;

fn add_collector_for_item(
    db: &Connection,
    item_id: i64,
    collector: &Collector,
) -> Result<bool, Error> {
    add_collector(db, collector)?;
    // query returns value if not present
    let mut stmt = db
        .prepare_cached(INSERT_COLLECTED_BY)
        .context(DbPrepareSnafu)?;
    let res = stmt
        .query((item_id, collector.fan_id))
        .context(DbWriteSnafu)?
        .next()
        .context(DbReadSnafu)?
        .is_none();
    Ok(res)
}

struct PageResults {
    token: String,
    album_id: i64,
    album_type: String,
}

lazy_static! {
    static ref BANDCAMP_REGEX: Regex = Regex::new("^https?://[a-z0-9-]+\\.bandcamp\\.com").unwrap();
}

async fn get_initial_page(
    db: &Pool<SqliteConnectionManager>,
    item: &Item,
) -> Result<Option<PageResults>, Error> {
    println!("Fetching collectors for {}", item.item_title);
    // Not a bandcamp url
    if !BANDCAMP_REGEX.is_match(&item.item_url) {
        return Err(Error::NotFoundError);
    }
    let client = Client::new();
    let page = client
        .get(&item.item_url)
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
    let item_id = item.item_id;
    let db = db.clone();
    let result = spawn_blocking(move || {
        let soup = Soup::new(&body);
        let node = soup.attr("id", "collectors-data").find();
        let Some(node) = node else {
            return Err(
                if soup
                    .attr("id", "subscription-collectors-data")
                    .find()
                    .is_some()
                {
                    // I have no clue how to properly scrape subscriptions, so just ignore them
                    Error::NotFoundError
                } else {
                    Error::PageError
                },
            );
        };
        let attrs = node.attrs();
        let body = attrs.get("data-blob").context(PageSnafu)?;
        let collectors: CollectorsData = serde_json::from_str(body).context(SerializationSnafu)?;
        let mut token = "".to_string();
        let mut done = false;
        let conn = db.get().context(DbPoolSnafu)?;
        for collector in collectors.thumbs {
            done = add_collector_for_item(&conn, item_id, &collector)? || done;
            if let Some(current_token) = collector.token {
                token = current_token;
            }
        }
        if !done && collectors.more_thumbs_available {
            let node = soup
                .attr("name", "bc-page-properties")
                .find()
                .context(PageSnafu)?;
            let attrs = node.attrs();
            let body = attrs.get("content").context(PageSnafu)?;
            let album_page_data: AlbumProperties =
                serde_json::from_str(body).context(SerializationSnafu)?;
            let album_id = album_page_data.item_id;
            let album_type = album_page_data.item_type;
            Ok(Some(PageResults {
                token,
                album_id,
                album_type,
            }))
        } else {
            Ok(None)
        }
    })
    .await
    .unwrap()?;
    Ok(result)
}

async fn get_next_page(
    db: &Pool<SqliteConnectionManager>,
    item: &Item,
    page_result: &PageResults,
) -> Result<Option<String>, Error> {
    println!("Fetching more collectors for {}", item.item_title);
    let client = Client::new();
    let result = client
        .post("https://bandcamp.com/api/tralbumcollectors/2/thumbs")
        .body(
            json!({
                "count": 500,
                "token": page_result.token,
                "tralbum_id": page_result.album_id,
                "tralbum_type": page_result.album_type,
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
    let item_id = item.item_id;
    let db = db.clone();
    let mut token = page_result.token.clone();
    spawn_blocking(move || {
        let collectors: CollectorsResult =
            serde_json::from_str(&body).context(SerializationSnafu)?;
        let mut done = false;
        let conn = db.get().context(DbPoolSnafu)?;
        for collector in collectors.results {
            done = add_collector_for_item(&conn, item_id, &collector)? || done;
            if let Some(current_token) = collector.token {
                token = current_token;
            }
        }
        if !done && collectors.more_available {
            Ok(Some(token))
        } else {
            Ok(None)
        }
    })
    .await
    .unwrap()
}

pub async fn fetch_track_collectors(
    db: &Pool<SqliteConnectionManager>,
    item_id: i64,
) -> Result<(), Error> {
    let conn = db.get().context(DbPoolSnafu)?;
    if item_present_and_recent(&conn, item_id)? {
        return Ok(());
    }
    let item = get_item(&conn, item_id)?;
    drop(conn);
    let result = get_initial_page(db, &item).await?;
    if let Some(mut result) = result {
        while let Some(token) = get_next_page(db, &item, &result).await? {
            result.token = token;
        }
    }
    Ok(())
}

const SELECT_FIRST_QUEUE_ITEM: &str = r#"
select item_id from item_collected_by_queue
order by item_id asc
limit 1"#;

const SELECT_UNFINISHED: &str = r#"
select item_id from item
where unixepoch('now') > unixepoch(last_updated, '30 days')
order by item_id asc
limit 1"#;

fn get_next_item(db: &Connection, crawl: bool) -> Result<Option<i64>, Error> {
    let mut stmt = db
        .prepare_cached(SELECT_FIRST_QUEUE_ITEM)
        .context(DbPrepareSnafu)?;
    let mut rows = stmt.query([]).context(DbReadSnafu)?;
    let row = rows.next().context(DbReadSnafu)?;
    if let Some(row) = row {
        let item_id: i64 = row.get("item_id").context(DbReadSnafu)?;
        Ok(Some(item_id))
    } else if crawl {
        let mut stmt = db
            .prepare_cached(SELECT_UNFINISHED)
            .context(DbPrepareSnafu)?;
        let mut rows = stmt.query([]).context(DbReadSnafu)?;
        let row = rows.next().context(DbReadSnafu)?;
        if let Some(row) = row {
            let item_id: i64 = row.get("item_id").context(DbReadSnafu)?;
            Ok(Some(item_id))
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

const MARK_ITEM_DONE: &str = r#"
update item
set last_updated = unixepoch('now')
where item_id = ?"#;

fn mark_item_done(db: &Connection, item_id: i64) -> Result<(), Error> {
    let mut stmt = db.prepare_cached(MARK_ITEM_DONE).context(DbPrepareSnafu)?;
    stmt.execute([item_id]).context(DbWriteSnafu)?;
    Ok(())
}

const DELETE_QUEUE_ITEM: &str = r#"
delete from item_collected_by_queue where item_id = ?"#;

fn remove_from_queue(db: &Connection, item_id: i64) -> Result<(), Error> {
    let mut stmt = db
        .prepare_cached(DELETE_QUEUE_ITEM)
        .context(DbPrepareSnafu)?;
    stmt.execute([item_id]).context(DbWriteSnafu)?;
    Ok(())
}

const DELETE_COLLECTED_BY: &str = r#"
delete from collected_by where item_id = ?"#;

fn remove_collected_by(db: &Connection, item_id: i64) -> Result<(), Error> {
    let mut stmt = db
        .prepare_cached(DELETE_COLLECTED_BY)
        .context(DbPrepareSnafu)?;
    stmt.execute([item_id]).context(DbWriteSnafu)?;
    Ok(())
}

pub async fn item_worker(
    db: &Pool<SqliteConnectionManager>,
    crawl: bool,
    run_state: &AtomicBool,
) -> Result<(), Error> {
    let mut timer = interval(Duration::from_secs(3));
    timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
    while run_state.load(Ordering::Relaxed) {
        let conn = db.get().context(DbPoolSnafu)?;
        if let Some(item_id) = get_next_item(&conn, crawl)? {
            drop(conn);
            match fetch_track_collectors(db, item_id).await {
                Err(Error::RateLimit) => {
                    println!("Rate limited, waiting 10 seconds");
                    let conn = db.get().context(DbPoolSnafu)?;
                    remove_collected_by(&conn, item_id)?;
                    sleep(Duration::from_secs(10)).await
                }
                Err(Error::NotFoundError) => {
                    println!("Item with id {item_id} not found");
                    let conn = db.get().context(DbPoolSnafu)?;
                    mark_item_done(&conn, item_id)?;
                    remove_from_queue(&conn, item_id)?;
                }
                Err(err) => {
                    println!("Error while processing item {item_id}: {err}");
                }
                Ok(()) => {
                    let conn = db.get().context(DbPoolSnafu)?;
                    mark_item_done(&conn, item_id)?;
                    remove_from_queue(&conn, item_id)?;
                }
            }
        }
        timer.tick().await;
    }
    Ok(())
}
