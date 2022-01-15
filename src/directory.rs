use std::error::Error;
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub enum DirectoryError {
    IOError,
    NotADirectory,
}

impl Error for DirectoryError {}

impl fmt::Display for DirectoryError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DirectoryError::IOError => write!(f, "IO error when reading directory"),
            DirectoryError::NotADirectory => write!(f, "Path given is not a directory"),
        }
    }
}

pub type DirectoryResult<T> = Result<T, DirectoryError>;

#[derive(Debug)]
pub struct Directory {
    path: PathBuf,
}

impl Directory {
    pub fn new(path: &str) -> DirectoryResult<Directory> {
        let dir_path = Path::new(path);
        if !dir_path.is_dir() {
            return Err(DirectoryError::NotADirectory);
        }

        Ok(Self {
            path: dir_path.to_path_buf(),
        })
    }

    pub fn get_file_list(&self, extension: &str) -> DirectoryResult<Vec<PathBuf>> {
        let mut list = vec![];

        let result = self.visit_files();
        match result {
            Ok(entries) => {
                for entry in entries {
                    let file_extension = entry
                        .extension()
                        .or_else(|| Some(std::ffi::OsStr::new("")))
                        .unwrap();
                    if !entry.is_dir() && file_extension == extension {
                        list.push(entry);
                    }
                }
            }
            Err(_) => return DirectoryResult::Err(DirectoryError::IOError),
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
