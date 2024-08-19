use std::io::{ErrorKind, SeekFrom};
use std::path::Path;

use tokio::fs::File;
use tokio::io;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

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

pub async fn write(file: &mut File, position: u64, buffer: &[u8]) -> io::Result<()> {
    file.seek(SeekFrom::Start(position)).await?;
    file.write_all(buffer).await
}

pub async fn write_end(file: &mut File, buffer: &[u8]) -> io::Result<()> {
    file.seek(SeekFrom::End(0)).await?;
    file.write_all(buffer).await
}

pub async fn seek(file: &mut File, position: u64, buffer: &mut [u8]) -> io::Result<()> {
    file.seek(SeekFrom::Start(position)).await?;
    file.read_exact(buffer).await.map(|_| ())
}

pub async fn seek_or(
    file: &mut File,
    position: u64,
    buffer: &mut [u8],
    default: &[u8],
) -> io::Result<()> {
    file.seek(SeekFrom::Start(position)).await?;
    if let Err(error) = file.read_exact(buffer).await {
        if error.kind() == ErrorKind::UnexpectedEof {
            return file.write_all(default).await;
        }

        return Err(error);
    }

    Ok(())
}
