use crate::types::{target_from_row, Target};
use crate::{DbPoolSnafu, DbPrepareSnafu, DbReadSnafu, DbWriteSnafu, Error};
use fallible_iterator::FallibleIterator;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{CachedStatement, Connection};
use snafu::ResultExt;
use std::time::Duration;
use tokio::time::{interval, MissedTickBehavior};

const STAGE_1_PER_ITEM: usize = 2;
const STAGE_2_PER_ITEM: usize = 3;

fn get_ids(fan_id: i64, stmt: &mut CachedStatement) -> Result<Vec<i64>, Error> {
    let results = stmt
        .query([fan_id])
        .context(DbReadSnafu)?
        .map(|row| row.get(0))
        .collect::<Vec<i64>>()
        .context(DbReadSnafu)?;
    Ok(results)
}

const SELECT_PENDING_STAGE_1_REQUIREMENTS: &str = r#"
select item_id from collects c
where fan_id = ? and
(select unixepoch('now') > unixepoch(last_updated, '30 days') from item i where i.item_id = c.item_id)
"#;

fn get_stage_1_requirements(db: &Connection, fan_id: i64) -> Result<Vec<i64>, Error> {
    let mut stmt = db
        .prepare_cached(SELECT_PENDING_STAGE_1_REQUIREMENTS)
        .context(DbPrepareSnafu)?;
    let results = get_ids(fan_id, &mut stmt)?;
    Ok(results)
}

const SELECT_PENDING_STAGE_2_REQUIREMENTS: &str = r#"
select fan_id from collected_by c
where item_id in (select item_id from collects where fan_id = ?) and
(select unixepoch('now') > unixepoch(last_updated, '30 days') from collector co where co.fan_id = c.fan_id)
group by fan_id
having count(fan_id) > 1"#;

fn get_stage_2_requirements(db: &Connection, fan_id: i64) -> Result<Vec<i64>, Error> {
    let mut stmt = db
        .prepare_cached(SELECT_PENDING_STAGE_2_REQUIREMENTS)
        .context(DbPrepareSnafu)?;
    let results = get_ids(fan_id, &mut stmt)?;
    Ok(results)
}

const INSERT_TARGET: &str = r#"
insert into collection_target (fan_id, stage, count_left, count_total, eta)
values (?, ?, ?, ?, ?)
on conflict do update
set stage = excluded.stage,
    count_left = excluded.count_left,
    count_total = case when excluded.count_total > count_total then excluded.count_total else count_total end,
    eta = excluded.eta
"#;

fn insert_target(
    db: &Connection,
    fan_id: i64,
    stage: i64,
    count_left: usize,
    count_total: usize,
    eta: usize,
) -> Result<(), Error> {
    let mut stmt = db.prepare_cached(INSERT_TARGET).context(DbPrepareSnafu)?;
    stmt.execute((fan_id, stage, count_left, count_total, eta))
        .context(DbWriteSnafu)?;
    Ok(())
}

const INSERT_TO_COLLECTED_BY_QUEUE: &str = r#"
insert or ignore into item_collected_by_queue (item_id) values (?)"#;

fn insert_to_collected_by_queue(db: &Connection, item_id: i64) -> Result<(), Error> {
    let mut stmt = db
        .prepare_cached(INSERT_TO_COLLECTED_BY_QUEUE)
        .context(DbPrepareSnafu)?;
    stmt.execute([item_id]).context(DbWriteSnafu)?;
    Ok(())
}

const INSERT_TO_COLLECTION_QUEUE: &str = r#"
insert or ignore into collector_collection_queue (fan_id) values (?)"#;

fn insert_to_collection_queue(db: &Connection, fan_id: i64) -> Result<(), Error> {
    let mut stmt = db
        .prepare_cached(INSERT_TO_COLLECTION_QUEUE)
        .context(DbPrepareSnafu)?;
    stmt.execute([fan_id]).context(DbWriteSnafu)?;
    Ok(())
}

const SELECT_TARGET: &str = r#"
select * from collection_target where fan_id = ?"#;

fn get_target(db: &Connection, fan_id: i64) -> Result<Target, Error> {
    let mut stmt = db.prepare_cached(SELECT_TARGET).context(DbPrepareSnafu)?;
    let result = stmt
        .query([fan_id])
        .context(DbReadSnafu)?
        .next()
        .context(DbReadSnafu)?
        .map(target_from_row)
        .unwrap_or(Ok(Target {
            fan_id,
            stage: 3,
            count_left: 0,
            count_total: 0,
            eta: 0,
        }))
        .context(DbReadSnafu)?;
    Ok(result)
}

const DELETE_TARGET: &str = r#"
delete from collection_target where fan_id = ?"#;

fn delete_target(db: &Connection, fan_id: i64) -> Result<(), Error> {
    let mut stmt = db.prepare_cached(DELETE_TARGET).context(DbPrepareSnafu)?;
    stmt.execute([fan_id]).context(DbWriteSnafu)?;
    Ok(())
}

fn handle_stage_2(db: &Connection, fan_id: i64, old_count: Option<i64>) -> Result<(), Error> {
    let requirements = get_stage_2_requirements(db, fan_id)?;
    if !requirements.is_empty() {
        insert_target(
            db,
            fan_id,
            2,
            requirements.len(),
            old_count.map(|v| v as usize).unwrap_or(requirements.len()),
            requirements.len() * STAGE_2_PER_ITEM,
        )?;
        if old_count.is_none() {
            for fan_id in requirements {
                insert_to_collection_queue(db, fan_id)?;
            }
        }
    } else {
        delete_target(db, fan_id)?;
    }
    Ok(())
}

fn handle_stage_1(db: &Connection, fan_id: i64, old_count: Option<i64>) -> Result<(), Error> {
    let requirements = get_stage_1_requirements(db, fan_id)?;
    if !requirements.is_empty() {
        insert_target(
            db,
            fan_id,
            1,
            requirements.len(),
            old_count.map(|v| v as usize).unwrap_or(requirements.len()),
            requirements.len() * STAGE_1_PER_ITEM,
        )?;
        if old_count.is_none() {
            for item_id in requirements {
                insert_to_collected_by_queue(db, item_id)?;
            }
        }
    } else {
        handle_stage_2(db, fan_id, None)?;
    }
    Ok(())
}

pub fn add_target(db: &Connection, fan_id: i64) -> Result<Target, Error> {
    handle_stage_1(db, fan_id, None)?;
    get_target(db, fan_id)
}

pub fn update_target(db: &Connection, fan_id: i64) -> Result<(), Error> {
    let target = get_target(db, fan_id)?;
    if target.stage == 1 {
        handle_stage_1(db, fan_id, Some(target.count_total))?;
    } else if target.stage == 2 {
        handle_stage_2(db, fan_id, Some(target.count_total))?;
    }
    Ok(())
}

const GET_TARGETS: &str = r#"
select fan_id from collection_target"#;

fn get_targets(db: &Connection) -> Result<Vec<i64>, Error> {
    let mut stmt = db.prepare_cached(GET_TARGETS).context(DbPrepareSnafu)?;
    let result = stmt
        .query([])
        .context(DbReadSnafu)?
        .map(|row| row.get(0))
        .collect::<Vec<i64>>()
        .context(DbReadSnafu)?;
    Ok(result)
}

pub async fn progress_manager(db: &Pool<SqliteConnectionManager>) -> Result<(), Error> {
    let mut timer = interval(Duration::from_secs(1));
    timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        let conn = db.get().context(DbPoolSnafu)?;
        for target in get_targets(&conn)? {
            update_target(&conn, target)?
        }
        drop(conn);
        timer.tick().await;
    }
}
