use std::path::Path;

use tokio::fs::File;
use tokio::io;

pub async fn create_and_open_file<P: AsRef<Path>>(file_name: &str, path: P) -> io::Result<File> {
    let file_path = path.as_ref().join(format!("{}.dsto", file_name));
    let Ok(file) = File::create_new(file_path.clone()).await else {
        return File::options().read(true).write(true).open(file_path).await;
    };

    Ok(file)
}
