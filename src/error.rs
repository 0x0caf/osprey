use std::error::Error;
use std::fmt;
use std::fmt::Display;

quick_error! {
    #[derive(Debug)]
    pub enum OspreyError {
        NotADirectory{
            display("Not a directory")
        }
        SQLFileError(err: SQLFileError) {
            source(err)
            from()
        }
        Postgres(err: postgres::Error) {
            source(err)
            from()
        }
        Io(err: std::io::Error) {
            source(err)
            from()
        }
        Sanity(err: SanityError){
            source(err)
            from()
        }
    }
}

#[derive(Debug)]
pub enum SQLFileError {
    SyntaxError(i32, String),
    CouldNoReadFile,
    CouldNotGetFilename,
}

impl Error for SQLFileError {}

impl fmt::Display for SQLFileError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SQLFileError::SyntaxError(line, m) => write!(
                f,
                "SQL File contains a syntax error. Line: {} - {}",
                line, m
            ),
            SQLFileError::CouldNoReadFile => write!(f, "Could not read file"),
            SQLFileError::CouldNotGetFilename => {
                write!(f, "Could not determine file's stem name from path")
            }
        }
    }
}

#[derive(Debug)]
pub enum SanityError {
    FileNoContainTag(String, String),
    FileQuerySetChanged(String, String),
    FileNoExist(String),
    FileNotMigrated(String),
}

impl Error for SanityError {}

impl Display for SanityError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SanityError::FileNoContainTag(file, tag) => write!(
                f,
                "The file {} does not contain the tag {} that was originally migrated",
                file, tag
            ),
            SanityError::FileQuerySetChanged(file, tag) => write!(
                f,
                "The file {} has changed since it was last migrated with the tag {}",
                file, tag
            ),
            SanityError::FileNoExist(file) => write!(
                f,
                "The file {} does not exist but exists in the migration table",
                file
            ),
            SanityError::FileNotMigrated(file) => {
                write!(f, "The file {} does not exist in the migration table", file)
            }
        }
    }
}
