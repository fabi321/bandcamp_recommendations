use crate::items::get_item;
use crate::types::Item;
use crate::{DbPrepareSnafu, DbReadSnafu, Error, NotFoundSnafu};
use fallible_iterator::FallibleIterator;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::Connection;
use snafu::{OptionExt, ResultExt};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

const SELECT_RELEVANT_USERS: &str = r#"
select fan_id, group_concat(item_id) from collects
where fan_id in (
    select fan_id from collects
    where item_id in (
        select item_id from collects
        where fan_id = (
            select fan_id from collector where username = ?
        )
    )
	group by fan_id
    having count(fan_id) > 1
)
group by fan_id"#;

fn get_relevant_users(db: &Connection, name: &str) -> Result<HashMap<i64, HashSet<i64>>, Error> {
    let mut stmt = db
        .prepare_cached(SELECT_RELEVANT_USERS)
        .context(DbPrepareSnafu)?;
    let result = stmt
        .query([name])
        .context(DbReadSnafu)?
        .map(|r| {
            Ok((
                r.get(0)?,
                r.get::<_, String>(1)?
                    .split(',')
                    .map(|v| v.parse().unwrap())
                    .collect(),
            ))
        })
        .collect()
        .context(DbReadSnafu)?;
    Ok(result)
}

pub fn get_user_recommendations(
    db: &Pool<SqliteConnectionManager>,
    username: &str,
    similar_boost: f64,
) -> Result<Vec<Item>, Error> {
    let conn = db.get().unwrap();
    let fan_id =
        crate::collectors::get_fan_id_for_username(&conn, username)?.context(NotFoundSnafu)?;
    let users = get_relevant_users(&conn, username)?;
    let forbidden = users[&fan_id].clone();
    let mut count: HashMap<i64, f64> = HashMap::new();
    for (_, user) in users {
        let mult = (user.intersection(&forbidden).count() as f64).powf(similar_boost);
        if mult > 1.0 {
            for item in user.difference(&forbidden) {
                let entry = count.entry(*item).or_default();
                *entry += mult;
            }
        }
    }
    let mut elements = count.into_iter().collect::<Vec<_>>();
    elements.sort_unstable_by(|(_, a), (_, b)| b.partial_cmp(a).unwrap_or(Ordering::Equal));
    let mut result = Vec::new();
    for (item_id, score) in elements.into_iter().take(50) {
        let mut item = get_item(&conn, item_id)?;
        item.score = Some(score);
        result.push(item)
    }
    Ok(result)
}
