use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::error::OspreyError;

#[derive(Debug)]
pub struct Directory {
    path: PathBuf,
}

impl Directory {
    pub fn new(path: &str) -> Result<Directory, OspreyError> {
        let dir_path = Path::new(path);
        if !dir_path.is_dir() {
            return Err(OspreyError::NotADirectory);
        }

        Ok(Self {
            path: dir_path.to_path_buf(),
        })
    }

    pub fn get_file_list(&self, extension: &str) -> Result<Vec<PathBuf>, OspreyError> {
        let mut list = vec![];

        let entries = self.visit_files()?;
        for entry in entries {
            let file_extension = entry
                .extension()
                .or_else(|| Some(std::ffi::OsStr::new("")))
                .unwrap();
            if !entry.is_dir() && file_extension == extension {
                list.push(entry);
            }
        }

        Ok(list)
    }

    fn visit_files(&self) -> io::Result<Vec<PathBuf>> {
        let entries = fs::read_dir(&self.path)?
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<PathBuf>, io::Error>>()?;

        Ok(entries)
    }
}
