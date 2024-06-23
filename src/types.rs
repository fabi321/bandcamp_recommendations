use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};
use rusqlite::Row;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ItemType {
    #[serde(rename = "album")]
    Album,
    #[serde(rename = "track")]
    Track,
    #[serde(rename = "package")]
    Package,
    #[serde(rename = "lepledge")]
    Lepledge,
    #[serde(rename = "subscription")]
    Subscription,
}

impl FromSql for ItemType {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Text(b"album") => Ok(ItemType::Album),
            ValueRef::Text(b"track") => Ok(ItemType::Track),
            ValueRef::Text(b"package") => Ok(ItemType::Package),
            ValueRef::Text(b"lepledge") => Ok(ItemType::Lepledge),
            ValueRef::Text(b"subscription") => Ok(ItemType::Subscription),
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

impl ToSql for ItemType {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(ValueRef::Text(match self {
            ItemType::Album => b"album",
            ItemType::Track => b"track",
            ItemType::Package => b"package",
            ItemType::Lepledge => b"lepledge",
            ItemType::Subscription => b"subscription",
        })))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Item {
    pub item_id: i64,
    pub item_type: ItemType,
    pub item_title: String,
    pub item_url: String,
    pub album_id: Option<i64>,
    pub album_title: Option<String>,
    pub band_id: i64,
    pub band_name: String,
    pub token: Option<String>,
    pub also_collected_count: i64,
}

pub fn item_from_row(row: &Row) -> rusqlite::Result<Item> {
    Ok(Item {
        item_id: row.get("item_id")?,
        item_type: row.get("item_type")?,
        item_title: row.get("item_title")?,
        item_url: row.get("item_url")?,
        album_id: None,
        album_title: None,
        band_id: row.get("band_id")?,
        band_name: row.get("band_name")?,
        token: row.get("token")?,
        also_collected_count: row.get("also_collected_count")?,
    })
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Collector {
    pub fan_id: i64,
    pub username: String,
    pub name: String,
    pub token: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Target {
    pub fan_id: i64,
    pub stage: i64,
    pub count_left: i64,
    pub count_total: i64,
    pub eta: i64,
}

pub fn target_from_row(row: &Row) -> rusqlite::Result<Target> {
    Ok(Target {
        fan_id: row.get("fan_id")?,
        stage: row.get("stage")?,
        count_left: row.get("count_left")?,
        count_total: row.get("count_total")?,
        eta: row.get("eta")?,
    })
}
