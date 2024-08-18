use std::collections::HashMap;
use std::io::{Read, Seek, Write};
use std::sync::Arc;

use axum::{Router, routing::post};
use tokio::sync::Mutex;

use crate::transport::api::{AppState, create_table, query_table};

mod dio;
mod table;
mod transport;

#[tokio::main]
async fn main() {
    let app_state = AppState {
        open_tables: Arc::new(Mutex::new(HashMap::new())),
    };

    let app = Router::new()
        .route("/create_table", post(create_table))
        .route("/query", post(query_table))
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

// fn create_table_structure(table_name: &str, columns: &[String]) -> dio::Result<()> {
//     let folder_path = Path::new(table_name);
//     std::fs::create_dir_all(folder_path)?;
//
//     for column in columns {
//         let file_path = folder_path.join(format!("{}.dsto", column));
//         if let Err(error) = File::create_new(file_path) {
//             println!("Error while creating table: {:?}", error);
//         };
//     }
//
//     Ok(())
// }
//
// fn write_value(table_name: &str, column_name: &str, value: i64, position: u64) -> dio::Result<()> {
//     let file_path = Path::new(table_name).join(format!("{}.dsto", column_name));
//     let mut file = OpenOptions::new().write(true).open(file_path)?;
//
//     file.seek(SeekFrom::Start(position * VALUE_SIZE as u64))?;
//     file.write_all(&value.to_le_bytes())?;
//
//     Ok(())
// }
//
// fn read_value(table_name: &str, column_name: &str, position: u64) -> dio::Result<i64> {
//     let file_path = Path::new(table_name).join(format!("{}.dsto", column_name));
//     let mut file = File::open(file_path)?;
//
//     file.seek(SeekFrom::Start(position * VALUE_SIZE as u64))?;
//     let mut buffer = [0u8; VALUE_SIZE];
//     file.read_exact(&mut buffer)?;
//
//     Ok(i64::from_le_bytes(buffer))
// }
