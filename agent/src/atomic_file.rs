/*
 * Copyright (C) 2026 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use log::error;
use std::fs::{remove_file, rename, OpenOptions};
use std::io::prelude::*;
use std::io::{Error, ErrorKind, Result};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

// This trait and implementation is inspired by the AtomicFile implementation in Android
// https://developer.android.com/reference/android/util/AtomicFile

pub trait AtomicFile {
    fn write_all(&self, bytes: &[u8]) -> Result<()>;
    fn read_all(&self) -> Result<Vec<u8>>;
}

#[derive(Debug)]
/// An AtomicFile uses two files: a base file and a new file.
/// The base file contains the data that is returned by AtomicFile::read_all().
/// The new file is used to stage changes to the base file. Once the updated contents
/// have been written to the new file and synced to storage, it is renamed as the base
/// file. If the rename operation is interrupted, the base file remains unchanged.
struct AtomicFileCollection {
    base_file_path: PathBuf,
    new_file_path: PathBuf,
}

#[derive(Debug)]
pub struct AtomicFileImpl {
    mutex: Mutex<AtomicFileCollection>,
}

fn remove_file_if_exists<P: AsRef<Path>>(path: P) -> Result<()> {
    if let Err(e) = remove_file(path) {
        if e.kind() != ErrorKind::NotFound {
            return Err(e);
        }
    }

    Ok(())
}

fn write_and_sync(atomic_file_collection: &AtomicFileCollection, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&atomic_file_collection.new_file_path)?;

    file.write_all(bytes)?;
    file.sync_all()?;
    rename(&atomic_file_collection.new_file_path, &atomic_file_collection.base_file_path)?;

    Ok(())
}

impl AtomicFileImpl {
    pub fn new(base_file_path: &Path) -> Result<Self> {
        let base_file_name = base_file_path.file_name().ok_or(Error::new(
            ErrorKind::InvalidInput,
            "Failed to read file name of AtomicFileImpl",
        ))?;
        let base_file_name_str = base_file_name.to_str().ok_or(Error::new(
            ErrorKind::InvalidInput,
            "Invalid unicode in file name of AtomicFileImpl",
        ))?;
        let new_file_path = base_file_path.with_file_name(format!("{base_file_name_str}.new"));

        let atomic_file_collection =
            AtomicFileCollection { base_file_path: base_file_path.to_path_buf(), new_file_path };

        Ok(Self { mutex: Mutex::new(atomic_file_collection) })
    }
}

impl AtomicFile for AtomicFileImpl {
    fn write_all(&self, bytes: &[u8]) -> Result<()> {
        let atomic_file_collection = self
            .mutex
            .lock()
            .map_err(|_| Error::other("Could not lock on the AtomicFileCollection"))?;

        if let Err(e) = write_and_sync(&atomic_file_collection, bytes) {
            let remove_result = remove_file_if_exists(&atomic_file_collection.new_file_path);

            if remove_result.is_err() {
                error!(
                    "Failed to remove new file {} when performing a write: {:?}",
                    &atomic_file_collection.new_file_path.display(),
                    e
                );
            }

            return Err(e);
        }

        Ok(())
    }

    fn read_all(&self) -> Result<Vec<u8>> {
        let atomic_file_collection = self
            .mutex
            .lock()
            .map_err(|_| Error::other("Could not lock on the AtomicFileCollection"))?;

        let mut file =
            OpenOptions::new().read(true).open(&atomic_file_collection.base_file_path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use tempfile::tempdir;

    const EXPECTED_BASE_DATA: &[u8] = b"base file data";
    const EXPECTED_NEW_DATA: &[u8] = b"new file data";
    const EXPECTED_WRITTEN_DATA: &[u8] = b"hello, world!";

    struct FileConfig {
        base_file: bool,
        new_file: bool,
    }

    #[derive(Debug)]
    struct TestContext {
        atomic_file: AtomicFileImpl,
        base_file_path: PathBuf,
        new_file_path: PathBuf,
    }

    fn setup_files(temp_dir_path: &Path, file_config: FileConfig) -> Result<TestContext> {
        let base_file_path = temp_dir_path.join("testfile.bin");
        let new_file_path = temp_dir_path.join("testfile.bin.new");

        if file_config.base_file {
            let mut base_file = File::create(&base_file_path).unwrap();
            base_file.write_all(EXPECTED_BASE_DATA).unwrap();
        }

        if file_config.new_file {
            let mut new_file = File::create(&new_file_path).unwrap();
            new_file.write_all(EXPECTED_NEW_DATA).unwrap();
        }

        let atomic_file = AtomicFileImpl::new(&base_file_path).unwrap();

        Ok(TestContext { atomic_file, base_file_path, new_file_path })
    }

    #[test]
    fn write_no_existing_files_all_contents_written_to_new_base_file() {
        let temp_dir = tempdir().unwrap();
        let ctx =
            setup_files(temp_dir.path(), FileConfig { base_file: false, new_file: false }).unwrap();

        let write_result = ctx.atomic_file.write_all(EXPECTED_WRITTEN_DATA);

        assert!(
            write_result.is_ok(),
            "write_all resulted in an error: {:?}",
            write_result.err().unwrap()
        );
        let mut base_file = File::open(&ctx.base_file_path).unwrap();
        let mut read_data: Vec<u8> = Vec::new();
        base_file.read_to_end(&mut read_data).unwrap();
        assert_eq!(read_data, EXPECTED_WRITTEN_DATA);
        assert!(!ctx.new_file_path.exists());
    }

    #[test]
    fn write_existing_base_file_all_contents_written_to_new_base_file() {
        let temp_dir = tempdir().unwrap();
        let ctx =
            setup_files(temp_dir.path(), FileConfig { base_file: true, new_file: false }).unwrap();

        let write_result = ctx.atomic_file.write_all(EXPECTED_WRITTEN_DATA);

        assert!(
            write_result.is_ok(),
            "write_all resulted in an error: {:?}",
            write_result.err().unwrap()
        );
        let mut base_file = File::open(ctx.base_file_path).unwrap();
        let mut read_data: Vec<u8> = Vec::new();
        base_file.read_to_end(&mut read_data).unwrap();
        assert_eq!(read_data, EXPECTED_WRITTEN_DATA);
        assert!(!ctx.new_file_path.exists());
    }

    #[test]
    fn write_existing_new_file_all_contents_written_to_new_base_file() {
        let temp_dir = tempdir().unwrap();
        let ctx =
            setup_files(temp_dir.path(), FileConfig { base_file: false, new_file: true }).unwrap();

        let write_result = ctx.atomic_file.write_all(EXPECTED_WRITTEN_DATA);

        assert!(
            write_result.is_ok(),
            "write_all resulted in an error: {:?}",
            write_result.err().unwrap()
        );
        let mut base_file = File::open(ctx.base_file_path).unwrap();
        let mut read_data: Vec<u8> = Vec::new();
        base_file.read_to_end(&mut read_data).unwrap();
        assert_eq!(read_data, EXPECTED_WRITTEN_DATA);
        assert!(!ctx.new_file_path.exists());
    }

    #[test]
    fn write_existing_base_and_new_files_all_contents_written_to_new_base_file() {
        let temp_dir = tempdir().unwrap();
        let ctx =
            setup_files(temp_dir.path(), FileConfig { base_file: true, new_file: true }).unwrap();

        let write_result = ctx.atomic_file.write_all(EXPECTED_WRITTEN_DATA);

        assert!(
            write_result.is_ok(),
            "write_all resulted in an error: {:?}",
            write_result.err().unwrap()
        );
        let mut base_file = File::open(ctx.base_file_path).unwrap();
        let mut read_data: Vec<u8> = Vec::new();
        base_file.read_to_end(&mut read_data).unwrap();
        assert_eq!(read_data, EXPECTED_WRITTEN_DATA);
        assert!(!ctx.new_file_path.exists());
    }

    #[test]
    fn read_all_no_file_exists_should_error() {
        let temp_dir = tempdir().unwrap();
        let ctx =
            setup_files(temp_dir.path(), FileConfig { base_file: false, new_file: false }).unwrap();

        let read_result = ctx.atomic_file.read_all();

        assert!(read_result.is_err());
    }

    #[test]
    fn read_all_only_new_file_exists_should_error() {
        let temp_dir = tempdir().unwrap();
        let ctx =
            setup_files(temp_dir.path(), FileConfig { base_file: false, new_file: true }).unwrap();
        let read_result = ctx.atomic_file.read_all();

        assert!(read_result.is_err());
    }

    #[test]
    fn read_all_only_base_file_exists_should_return_base_file_contents() {
        let temp_dir = tempdir().unwrap();
        let ctx =
            setup_files(temp_dir.path(), FileConfig { base_file: true, new_file: false }).unwrap();

        let read_result = ctx.atomic_file.read_all();

        assert!(
            read_result.is_ok(),
            "read_all resulted in an error: {:?}",
            read_result.err().unwrap()
        );
        assert_eq!(read_result.unwrap(), EXPECTED_BASE_DATA);
    }

    #[test]
    fn read_all_base_and_new_file_exists_should_return_base_file_contents() {
        let temp_dir = tempdir().unwrap();
        let ctx =
            setup_files(temp_dir.path(), FileConfig { base_file: true, new_file: true }).unwrap();

        let read_result = ctx.atomic_file.read_all();

        assert!(
            read_result.is_ok(),
            "read_all resulted in an error: {:?}",
            read_result.err().unwrap()
        );
        assert_eq!(read_result.unwrap(), EXPECTED_BASE_DATA);
    }
}
