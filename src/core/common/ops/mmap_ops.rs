use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

use memmap2::{Mmap, MmapMut};

use super::madvise;

pub const TEMP_FILE_EXTENSION: &str = "tmp";

pub fn create_and_ensure_length(path: &Path, length: u64) -> Result<File, io::Error> {
    if path.exists() {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            // Don't truncate because we explicitly set the length later
            .truncate(false)
            .open(path)?;
        file.set_len(length)?;

        Ok(file)
    } else {
        let temp_path = path.with_extension(TEMP_FILE_EXTENSION);
        {
            // create temporary file with the required length
            // Temp file is used to avoid situations, where crash happens between file creation and setting the length
            let temp_file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                // Don't truncate because we explicitly set the length later
                .truncate(false)
                .open(&temp_path)?;
            temp_file.set_len(length)?;
        }

        std::fs::rename(&temp_path, path)?;

        OpenOptions::new().read(true).write(true).create(false).truncate(false).open(path)
    }
}

pub fn open_read_mmap(path: &Path) -> Result<Mmap, io::Error> {
    let file = OpenOptions::new().read(true).write(false).append(true).create(true).truncate(false).open(path)?;

    let mmap = unsafe { Mmap::map(&file)? };
    madvise::madvise(&mmap, madvise::get_global())?;

    Ok(mmap)
}

pub fn open_write_mmap(path: &Path) -> Result<MmapMut, io::Error> {
    let file = OpenOptions::new().read(true).write(true).create(false).open(path)?;

    let mmap = unsafe { MmapMut::map_mut(&file)? };
    madvise::madvise(&mmap, madvise::get_global())?;

    Ok(mmap)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_create_and_ensure_length() {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test_file");

        // Test creating a new file
        let file = create_and_ensure_length(&file_path, 1024).unwrap();
        assert!(file_path.exists());
        assert_eq!(file.metadata().unwrap().len(), 1024);

        // Test opening an existing file
        let file = create_and_ensure_length(&file_path, 2048).unwrap();
        assert!(file_path.exists());
        assert_eq!(file.metadata().unwrap().len(), 2048);
    }

    #[test]
    fn test_open_read_mmap() {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test_file");

        // Create a file with some content
        fs::write(&file_path, "Hello, World!").unwrap();

        // Test opening the file as read-only memory map
        let mmap = open_read_mmap(&file_path).unwrap();
        assert_eq!(mmap.len(), 13);
        assert_eq!(&mmap[..], b"Hello, World!");
    }

    #[test]
    fn test_open_write_mmap() {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test_file");

        // Create a file with some content
        fs::write(&file_path, "Hello, World!").unwrap();

        // Test opening the file as writable memory map
        let mut mmap: MmapMut = open_write_mmap(&file_path).unwrap();
        assert_eq!(mmap.len(), 13);
        assert_eq!(&mmap[..], b"Hello, World!");

        // Modify the content through the memory map
        mmap[7..10].copy_from_slice(b"RuA");

        // Flush changes to the underlying file.
        // Calling this flush function is not necessary, as the mmap file will automatically sync.
        mmap.flush().unwrap();

        // Check the modified content in the memory map
        let cur_mmap_str = std::str::from_utf8(&mmap[..]).unwrap();
        println!("{}", cur_mmap_str);
        assert_eq!(cur_mmap_str, "Hello, RuAld!");
        assert_eq!(&mmap[..], b"Hello, RuAld!");

        // Check the modified content in the file
        let file_content = fs::read(&file_path).unwrap();
        assert_eq!(file_content, b"Hello, RuAld!");
    }
}
