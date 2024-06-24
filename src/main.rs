use actix_web::{get, web, App, HttpResponse, HttpServer};
use actix_web::http::header::ContentType;
use clap::Parser;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use serde::Deserialize;
use snafu::Snafu;
use tokio::task::spawn_blocking;
use tokio::{join, spawn};

mod analyze;
mod args;
mod collectors;
mod items;
mod progress_manager;
mod types;

type DataType = web::Data<Pool<SqliteConnectionManager>>;

#[derive(Deserialize)]
struct UserInfo {
    username: String,
}

#[get("/api/get_status")]
async fn get_status(query: web::Query<UserInfo>, data: DataType) -> HttpResponse {
    let conn = data.get().unwrap();
    let fan_id = match collectors::get_fan_id_for_username(&conn, &query.username) {
        Ok(Some(fan_id)) => fan_id,
        Ok(None) => {
            return HttpResponse::NotFound().body("User not found");
        }
        Err(err) => {
            println!("Error getting status for user: {err}");
            return HttpResponse::InternalServerError().body("Internal server error");
        }
    };
    match progress_manager::add_target(&conn, fan_id) {
        Ok(target) => HttpResponse::Ok().body(serde_json::to_string(&target).unwrap()),
        Err(err) => {
            println!("Error getting status for user: {err}");
            HttpResponse::InternalServerError().body("Internal server error")
        }
    }
}

#[get("/api/get_user")]
async fn get_user(query: web::Query<UserInfo>, data: DataType) -> HttpResponse {
    let result = collectors::fetch_collection(data.get_ref(), &query.username, true)
        .await
        .and_then(|_| collectors::get_collection_size(data.get_ref(), &query.username));
    match result {
        Ok(size) => {
            if size > 2 {
                HttpResponse::Ok().body("User fetched successfully")
            } else {
                HttpResponse::NotFound()
                    .body("User does not contain enough items (at least 2 required)")
            }
        }
        Err(Error::NotFoundError) => HttpResponse::NotFound().body("User not found"),
        Err(err) => {
            println!("Error fetching user: {err}");
            HttpResponse::InternalServerError().body("Internal server error")
        }
    }
}

#[derive(Deserialize)]
struct RecommendationInfo {
    username: String,
    similar_boost: Option<f64>,
}

#[get("/api/get_recommendations")]
async fn get_recommendations(
    query: web::Query<RecommendationInfo>,
    data: DataType,
) -> HttpResponse {
    let similar_boost = query.similar_boost.unwrap_or(2.0).min(5.0).max(1.0);
    let result = spawn_blocking(move || {
        analyze::get_user_recommendations(data.get_ref(), &query.username, similar_boost)
    })
    .await
    .unwrap();
    match result {
        Ok(data) => HttpResponse::Ok().body(serde_json::to_string(&data).unwrap()),
        Err(Error::NotFoundError) => HttpResponse::NotFound().body("User not found"),
        Err(err) => {
            println!("Error getting recommendations for user: {err}");
            HttpResponse::InternalServerError().body("Internal server error")
        }
    }
}

#[get("/classless.css")]
async fn get_classless() -> HttpResponse {
    HttpResponse::Ok()
        .content_type(ContentType(mime::TEXT_CSS))
        .body(include_str!("../web_src/classless.css"))
}

#[get("/index.html")]
async fn get_index() -> HttpResponse {
    HttpResponse::Ok()
        .content_type(ContentType::html())
        .body(include_str!("../web_src/index.html"))
}

#[get("/")]
async fn get_root() -> HttpResponse {
    HttpResponse::Ok()
        .content_type(ContentType::html())
        .body(include_str!("../web_src/index.html"))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args = args::Args::parse();
    let manager = SqliteConnectionManager::file(&args.database);
    let pool = Pool::new(manager).expect("Unable to create sqlite pool");
    pool.get()
        .unwrap()
        .execute_batch(include_str!("init.sql"))
        .expect("Unable to initialize database");
    let db_copy = pool.clone();
    let collection_worker = spawn(async move {
        loop {
            let res = collectors::collection_worker(&db_copy, args.crawl).await.unwrap_err();
            println!("Error in collection_worker: {res}");
        }
    });
    let db_copy = pool.clone();
    let item_worker = spawn(async move {
        loop {
            let res = items::item_worker(&db_copy, args.crawl).await.unwrap_err();
            println!("Error in item_worker: {res}");
        }
    });
    let db_copy = pool.clone();
    let progress_manager = spawn(async move {
        loop {
            let res = progress_manager::progress_manager(&db_copy)
                .await
                .unwrap_err();
            println!("Error in progress_manager: {res}");
        }
    });
    let data = web::Data::new(pool.clone());
    let server = HttpServer::new(move || {
        App::new()
            .app_data(data.clone())
            .service(get_status)
            .service(get_user)
            .service(get_recommendations)
            .service(get_classless)
            .service(get_index)
            .service(get_root)
    })
        .bind(args.address)?
        .run();
    let res = join!(collection_worker, item_worker, progress_manager, server);
    res.0.unwrap();
    res.1.unwrap();
    res.2.unwrap();
    res.3.unwrap();
    Ok(())
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Network error: {:?}", source))]
    NetworkError { source: reqwest::Error },

    #[snafu(display("Resource not found"))]
    NotFoundError,

    #[snafu(display("Serialization error: {:?}", source))]
    SerializationError { source: serde_json::Error },

    #[snafu(display("Error opening database: {:?}", source))]
    DbOpenError { source: rusqlite::Error },

    #[snafu(display("Error preparing database statement: {:?}", source))]
    DbPrepareError { source: rusqlite::Error },

    #[snafu(display("Database read error: {:?}", source))]
    DbReadError { source: rusqlite::Error },

    #[snafu(display("Database write error: {:?}", source))]
    DbWriteError { source: rusqlite::Error },
    
    #[snafu(display("Error opening database: {:?}", source))]
    DbPoolError { source: r2d2::Error },

    #[snafu(display("No row returned from database"))]
    DbResultError,

    #[snafu(display("Rate limit reached"))]
    RateLimit,

    #[snafu(display("Page content error"))]
    PageError,
}
