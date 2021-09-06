use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fmt;
use std::fs::{self, File};
use std::io::{self, BufRead};
use std::path::{Path, PathBuf};

pub struct Env {}

impl Env {
    pub fn get_value_or_default(key: &str, default: &str) -> String {
        match env::var(key) {
            Ok(v) => v,
            Err(_) => default.to_string(),
        }
    }
}

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

#[derive(Clone, Debug)]
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
                        .or(Some(std::ffi::OsStr::new("")))
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

#[derive(Debug)]
pub enum SQLMigrationFileError {
    NotAFile,
    SyntaxError(String),
    MigrationUpMissing,
    MigrationDownMissing,
    CouldNotReadFile,
}

impl Error for SQLMigrationFileError {}

impl fmt::Display for SQLMigrationFileError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SQLMigrationFileError::NotAFile => write!(f, "Given path is not a file"),
            SQLMigrationFileError::SyntaxError(m) => {
                write!(f, "SQL File contains a syntax error: {}", m)
            }
            SQLMigrationFileError::CouldNotReadFile => write!(f, "Could not read file"),
            SQLMigrationFileError::MigrationUpMissing => {
                write!(f, "Migration up missing from file")
            }
            SQLMigrationFileError::MigrationDownMissing => {
                write!(f, "Migration down missing from file")
            }
        }
    }
}

pub struct FileLine {
    line: String,
}

const TAG_LINE: &str = "tag:";

impl FileLine {
    pub fn new(line: &str) -> FileLine {
        let new_line = line.trim().to_string();
        FileLine { line: new_line }
    }

    pub fn is_empty(&self) -> bool {
        self.line.is_empty()
    }

    pub fn is_tag_name(&self) -> bool {
        self.line.starts_with("--") && self.line.contains("tag:")
    }

    pub fn get_tag_name(&self) -> Option<String> {
        let indicies: Vec<_> = self.line.match_indices(TAG_LINE).collect();
        let tag_line_len = TAG_LINE.len();
        if indicies.len() > 0 {
            let first_index = indicies[0].0;
            let begin = first_index + tag_line_len;
            let mut tag = String::new();
            for i in begin..self.line.len() {
                tag.push(self.line.chars().nth(i).unwrap());
            }

            tag = tag.trim().to_string();
            if tag.is_empty() {
                return None;
            }
            return Some(tag);
        }

        None
    }

    pub fn is_comment_line(&self) -> bool {
        self.line.starts_with("--")
    }

    pub fn is_query_string(&self) -> bool {
        !self.is_empty() && !self.is_comment_line()
    }

    pub fn is_finishing_query(&self) -> bool {
        self.is_query_string() && self.line.ends_with(";")
    }
}

pub type SQLMigrationFileResult<T> = Result<T, SQLMigrationFileError>;

pub struct SQLMigrationFile {
    pub query_hash_map: HashMap<String, QuerySet>,
}

#[derive(Clone)]
pub struct QuerySet {
    queries: Vec<String>,
    current_query: String,
}

impl QuerySet {
    fn new() -> QuerySet {
        QuerySet {
            queries: vec![],
            current_query: String::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.queries.len() == 0
    }

    fn has_unfinished_query(&self) -> bool {
        !self.current_query.is_empty()
    }

    fn add_query_string(&mut self, st: &str) {
        println!("Add query string: {}", st);
        self.current_query.push_str(st);
    }

    fn finish_query(&mut self, st: &str) {
        println!("Finishing query with: {}", st);
        self.current_query.push_str(st);
        self.queries.push(self.current_query.clone());
        self.current_query = String::new();
    }
}

impl SQLMigrationFile {
    pub fn new(path: &PathBuf) -> SQLMigrationFileResult<SQLMigrationFile> {
        let mut tag_name = String::new();

        let mut query_hash_map = HashMap::new();

        let mut current_query_set = QuerySet::new();
        let mut line_count = 0;
        if let Ok(lines) = read_lines(path) {
            for maybe_line in lines {
                if let Ok(line) = maybe_line {
                    let file_line = FileLine::new(line.as_str());

                    if file_line.is_finishing_query() {
                        if tag_name.is_empty() {
                            return Err(SQLMigrationFileError::SyntaxError(
                                "Query defined without tag name".to_string(),
                            ));
                        }

                        current_query_set.finish_query(file_line.line.as_str());
                        continue;
                    }

                    if file_line.is_tag_name() {
                        if current_query_set.has_unfinished_query() {
                            return Err(SQLMigrationFileError::SyntaxError(
                                format!(
                                    "Line {} - Tag name defined without completing previous query",
                                    line_count
                                )
                                .to_string(),
                            ));
                        }

                        if !current_query_set.is_empty() {
                            query_hash_map.insert(tag_name, current_query_set.clone());
                            current_query_set = QuerySet::new();
                        }

                        match file_line.get_tag_name() {
                            Some(t) => tag_name = t,
                            None => {
                                return Err(SQLMigrationFileError::SyntaxError(format!(
                                    "Line {} - Could not parse tag name",
                                    line_count
                                )))
                            }
                        }
                        continue;
                    }

                    if file_line.is_comment_line() {
                        if current_query_set.has_unfinished_query() {
                            return Err(SQLMigrationFileError::SyntaxError(
                                format!("Line {} - Comment found while defining query", line_count)
                                    .to_string(),
                            ));
                        }
                        continue;
                    }

                    if file_line.is_empty() && current_query_set.has_unfinished_query() {
                        return Err(SQLMigrationFileError::SyntaxError(
                            format!(
                                "Line {} - Empty line in query: {}",
                                line_count, current_query_set.current_query
                            )
                            .to_string(),
                        ));
                    }

                    if file_line.is_query_string() {
                        current_query_set.add_query_string(file_line.line.as_str());
                    }
                }
                line_count = line_count + 1;
            }
        }

        if current_query_set.has_unfinished_query() {
            return Err(SQLMigrationFileError::SyntaxError(
                format!("Line {} - End of file found: unfinished query", line_count).to_string(),
            ));
        }

        if tag_name.is_empty() && current_query_set.is_empty() {
            return Err(SQLMigrationFileError::SyntaxError(
                format!("Line {} - No queries found in file", line_count).to_string(),
            ));
        }

        query_hash_map.insert(tag_name, current_query_set.clone());
        Ok(SQLMigrationFile { query_hash_map })
    }
}

fn read_lines<P>(path: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(path)?;
    Ok(io::BufReader::new(file).lines())
}

fn main() -> DirectoryResult<()> {
    let migrations_directory = Env::get_value_or_default("MIGRATIONS_DIRECTORY", "./migrations/");

    let directory_files = Directory::new(migrations_directory.as_str())?.get_file_list("sql")?;

    for file in directory_files {
        match file.to_str() {
            Some(s) => {
                println!("File: {}", s);
                match SQLMigrationFile::new(&file) {
                    Ok(f) => {
                        for (k, v) in &f.query_hash_map {
                            println!(" tag: {}", k);
                            for q in &v.queries {
                                println!("     query: {}", q);
                            }
                        }
                    }
                    Err(e) => {
                        println!("Error Occurred Parsing File:{}", e);
                    }
                }
            }
            None => println!("Not A File"),
        }
    }

    Ok(())
}
