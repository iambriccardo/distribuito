use std::io::ErrorKind;
use std::path::Path;
use tokio::fs::File;
use tokio::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufStream};

pub async fn create_file<P: AsRef<Path>>(file_name: &str, path: P) -> io::Result<()> {
    let file_path = path.as_ref().join(file_name);
    if let Err(error) = File::create_new(file_path.clone()).await {
        if error.kind() == ErrorKind::AlreadyExists {
            return Ok(());
        }

        return Err(error);
    };

    Ok(())
}

pub async fn create_and_open_file<P: AsRef<Path>>(file_name: &str, path: P) -> io::Result<File> {
    let file_path = path.as_ref().join(file_name);
    let Ok(file) = File::create_new(file_path.clone()).await else {
        return File::options().read(true).write(true).open(file_path).await;
    };

    Ok(file)
}

pub async fn open_append_file<P: AsRef<Path>>(file_name: &str, path: P) -> io::Result<File> {
    let file_path = path.as_ref().join(file_name);
    File::options().append(true).open(file_path).await
}

pub async fn open_read_file<P: AsRef<Path>>(file_name: &str, path: P) -> io::Result<File> {
    let file_path = path.as_ref().join(file_name);
    File::options().read(true).open(file_path).await
}

pub async fn read_or(
    file: &mut BufStream<File>,
    buffer: &mut [u8],
    default: &[u8],
) -> io::Result<()> {
    if let Err(error) = file.read_exact(buffer).await {
        if error.kind() == ErrorKind::UnexpectedEof {
            return file.write_all(default).await;
        }

        return Err(error);
    }

    Ok(())
}
