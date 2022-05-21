use crate::error::SQLFileError;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::path::Path;
use std::str;

const TAG_LINE: &str = "tag:";

#[derive(Debug, PartialEq)]
pub enum SyntaxErrorMessage {
    QueryGivenNoTag,
    TagNameIncompleteQuery,
    NoQueryForTag,
    CouldNotParseTagName,
    CommentInQuery,
    EOFIncompleteQuery,
    NoQueriesFound,
}

impl fmt::Display for SyntaxErrorMessage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SyntaxErrorMessage::QueryGivenNoTag => write!(f, "Query defined without tag name"),
            SyntaxErrorMessage::TagNameIncompleteQuery => {
                write!(f, "Tag name defined without completing previous query")
            }
            SyntaxErrorMessage::NoQueryForTag => write!(f, "No query given for tag"),
            SyntaxErrorMessage::CouldNotParseTagName => write!(f, "Could not parse tag name"),
            SyntaxErrorMessage::CommentInQuery => write!(f, "Comment found while defining query"),
            SyntaxErrorMessage::EOFIncompleteQuery => {
                write!(f, "End of file found: unfinished query")
            }
            SyntaxErrorMessage::NoQueriesFound => write!(f, "No queries found"),
        }
    }
}

impl SyntaxErrorMessage {
    fn to_err<T>(&self, line_number: i32) -> Result<T, SQLFileError> {
        Err(SQLFileError::SyntaxError(line_number, format!("{}", &self)))
    }
}
pub type SQLFileResult<T> = Result<T, SQLFileError>;
pub type Tag = String;

// SQLFile holds queries read from an sql file
//   the queries are grouped by the tag they are given in the file
#[derive(Debug)]
pub struct SQLFile {
    pub name: String,
    pub query_hash_map: HashMap<Tag, QuerySet>,
}

// QuerySet holds a groups of query strings as well as the computed hash of the entire set
#[derive(Debug)]
pub struct QuerySet {
    pub queries: Vec<String>,
    pub hash: String,
}

impl SQLFile {
    // reads sql from a file, ultimately uses, new_from_string
    pub fn new_from_file<P>(path: P) -> SQLFileResult<SQLFile>
    where
        P: AsRef<Path>,
    {
        if let Some(filename) = Self::file_stem(&path) {
            if let Ok(st) = fs::read_to_string(path) {
                return Self::new_from_string(&filename, &st);
            }

            return Err(SQLFileError::CouldNoReadFile);
        }
        Err(SQLFileError::CouldNotGetFilename)
    }

    // helper function to get the file's stem name: this_file.txt -> this_file
    fn file_stem<P>(path: P) -> Option<String>
    where
        P: AsRef<Path>,
    {
        let os_filename = path.as_ref().file_stem()?;
        let str_filename = os_filename.to_str()?;
        Some(str_filename.to_string())
    }

    // parses a string into a group of queries
    pub fn new_from_string(name: &str, text: &str) -> SQLFileResult<SQLFile> {
        let mut tag_name = String::new();
        let mut query_hash_map = HashMap::new();
        let mut current_query_set = QueryReadState::new();
        let mut line_count = 0;
        let lines = text.split('\n');

        for line in lines {
            let file_line = FileLine::new(line);

            if file_line.is_finishing_query() {
                if tag_name.is_empty() {
                    return SyntaxErrorMessage::QueryGivenNoTag.to_err(line_count);
                }

                current_query_set.finish_query(&file_line.original_line);
                continue;
            }

            if file_line.is_tag_name() {
                if current_query_set.has_unfinished_query() {
                    return SyntaxErrorMessage::TagNameIncompleteQuery.to_err(line_count);
                }

                if !tag_name.is_empty() && current_query_set.is_empty() {
                    return SyntaxErrorMessage::NoQueryForTag.to_err(line_count);
                }

                if !tag_name.is_empty() && !current_query_set.is_empty() {
                    let query_set = current_query_set.compute_hash().into_query_set();
                    query_hash_map.insert(tag_name, query_set);
                    current_query_set = QueryReadState::new();
                }

                match file_line.get_tag_name() {
                    Some(t) => tag_name = t,
                    None => {
                        return SyntaxErrorMessage::CouldNotParseTagName.to_err(line_count);
                    }
                }
                continue;
            }

            if file_line.is_comment_line() {
                if current_query_set.has_unfinished_query() {
                    return SyntaxErrorMessage::CommentInQuery.to_err(line_count);
                }
                continue;
            }

            if file_line.is_query_string() {
                current_query_set.add_query_string(&file_line.original_line);
            }
            line_count += 1;
        }

        if current_query_set.has_unfinished_query() {
            return SyntaxErrorMessage::EOFIncompleteQuery.to_err(line_count);
        }

        if tag_name.is_empty() && current_query_set.is_empty() {
            return SyntaxErrorMessage::NoQueriesFound.to_err(line_count);
        }

        if !tag_name.is_empty() && current_query_set.is_empty() {
            return SyntaxErrorMessage::NoQueryForTag.to_err(line_count);
        }

        let query_set = current_query_set.compute_hash().into_query_set();
        query_hash_map.insert(tag_name, query_set);
        Ok(SQLFile {
            name: name.to_string(),
            query_hash_map,
        })
    }
}

// FileLine gives basic descriptions of a line of a file
//   helps determine whether the parsing is in a comment, query, or somewhere in between
#[derive(Debug)]
struct FileLine {
    line: String,
    original_line: String,
}

impl FileLine {
    fn new(line: &str) -> FileLine {
        let new_line = line.trim().to_string();
        FileLine {
            line: new_line,
            original_line: line.to_string(),
        }
    }

    fn is_empty(&self) -> bool {
        self.line.is_empty()
    }

    fn is_tag_name(&self) -> bool {
        self.is_comment_line() && self.line.contains(TAG_LINE)
    }

    fn get_tag_name(&self) -> Option<String> {
        let indicies: Vec<_> = self.line.match_indices(TAG_LINE).collect();
        let tag_line_len = TAG_LINE.len();
        if !indicies.is_empty() {
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

    fn is_comment_line(&self) -> bool {
        self.line.starts_with("--")
    }

    fn is_query_string(&self) -> bool {
        !self.is_empty() && !self.is_comment_line()
    }

    fn is_finishing_query(&self) -> bool {
        self.is_query_string() && self.line.ends_with(';')
    }
}

// QueryReadState gives basic information of where in an sql query string the file parser is.
//   As this state determines if a query is finished it'll be pushed into the queries vector.
//   Current queries being read by the parser are held in current_query
#[derive(Debug)]
struct QueryReadState {
    queries: Vec<String>,
    current_query: String,
    hash: String,
}

impl QueryReadState {
    fn new() -> QueryReadState {
        QueryReadState {
            queries: vec![],
            current_query: String::new(),
            hash: "".to_string(),
        }
    }

    fn is_empty(&self) -> bool {
        self.queries.len() == 0
    }

    fn has_unfinished_query(&self) -> bool {
        !self.current_query.is_empty()
    }

    fn add_query_string(&mut self, st: &str) {
        if !self.current_query.is_empty() {
            self.current_query.push('\n');
        }
        self.current_query.push_str(st);
    }

    fn finish_query(&mut self, st: &str) {
        self.add_query_string(st);
        self.queries.push(self.current_query.clone());
        self.current_query = String::new();
    }

    fn compute_hash(mut self) -> Self {
        let mut all_queries = String::new();

        for query in self.queries.iter() {
            all_queries.push_str(query);
        }

        let mut hasher = Sha256::new();
        hasher.update(all_queries);

        let hash = format!("{:X}", hasher.finalize());

        self.hash = hash;
        self
    }

    fn into_query_set(self) -> QuerySet {
        QuerySet {
            queries: self.queries,
            hash: self.hash,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn check_sem(result: Result<SQLFile, SQLFileError>, se: SyntaxErrorMessage) -> bool {
        assert!(result.is_err());
        if let SQLFileError::SyntaxError(_, err) = result.err().unwrap() {
            assert_eq!(err, format!("{}", se));
            return true;
        }
        false
    }

    #[test]
    fn test_valid_sql_file() {
        let valid_file = "\n-- tag:up \nSELECT * FROM atable WHERE *;";

        let sql_file = SQLFile::new_from_string("filename", valid_file);
        assert!(sql_file.is_ok());

        let file = sql_file.unwrap();
        assert_eq!(file.name.as_str(), "filename");
        assert!(file.query_hash_map.len() == 1);
        assert!(file.query_hash_map.contains_key(&"up".to_string()));
    }

    #[test]
    fn test_no_query_with_tag() {
        let no_query = "\n-- tag:up";

        let sql_file = SQLFile::new_from_string("filename", no_query);
        assert!(check_sem(sql_file, SyntaxErrorMessage::NoQueryForTag));
    }

    #[test]
    fn test_query_with_no_tag() {
        let no_tag = "SELECT * FROM atable WHERE *;";
        let maybe_sql_file = SQLFile::new_from_string("f", no_tag);
        assert!(check_sem(
            maybe_sql_file,
            SyntaxErrorMessage::QueryGivenNoTag
        ));
    }

    #[test]
    fn test_no_tag_name() {
        let no_tag_name = "\n-- tag:\nSELECT * FROM atable WHERE *;";
        let maybe_sql_file = SQLFile::new_from_string("f", no_tag_name);
        assert!(check_sem(
            maybe_sql_file,
            SyntaxErrorMessage::CouldNotParseTagName
        ));
    }

    #[test]
    fn test_unfinished_query() {
        let unfinished_query = "\n-- tag: up\nSELECT * FROM atable WHERE *";
        let maybe_sql_file = SQLFile::new_from_string("f", unfinished_query);
        assert!(check_sem(
            maybe_sql_file,
            SyntaxErrorMessage::EOFIncompleteQuery
        ));
    }

    #[test]
    fn test_query_set_no_query() {
        let queries = "\n-- tag:up\nSELECT * FROM atable WHERE *;\n-- tag:down\nSELECT * FROM atable WHERE *;\n-- tag:left\n-- tag:right\nSELECT * FROM atable WHERE *;";
        let maybe_sql_file = SQLFile::new_from_string("filename", queries);
        assert!(check_sem(maybe_sql_file, SyntaxErrorMessage::NoQueryForTag));
    }

    #[test]
    fn test_multiline_query() {
        let multiline: &str = "-- tag: up \nSELECT * FROM \natable WHERE \nacolumn=avalue;\n";

        let maybe_sql_file = SQLFile::new_from_string("f", multiline);
        assert!(maybe_sql_file.is_ok());

        let sql_file = maybe_sql_file.unwrap();
        let maybe_query = sql_file.query_hash_map.get("up");
        assert!(maybe_query.is_some());

        let set = maybe_query.unwrap();
        assert!(set.queries.len() == 1);
        assert_eq!(
            &set.queries[0],
            "SELECT * FROM \natable WHERE \nacolumn=avalue;"
        );
    }

    #[test]
    fn test_multiquery_set() {
        let queries = "\n-- tag:up\nSELECT * FROM onetable WHERE *;\nSELECT * FROM twotable WHERE *;\nSELECT * FROM threetable WHERE *;";

        let maybe_sql_file = SQLFile::new_from_string("f", queries);
        assert!(maybe_sql_file.is_ok());

        let sql_file = maybe_sql_file.unwrap();
        let maybe_query_set = sql_file.query_hash_map.get("up");
        assert!(maybe_query_set.is_some());

        let set = maybe_query_set.unwrap();
        assert!(set.queries.len() == 3);

        assert_eq!(&set.queries[0], "SELECT * FROM onetable WHERE *;");
        assert_eq!(&set.queries[1], "SELECT * FROM twotable WHERE *;");
        assert_eq!(&set.queries[2], "SELECT * FROM threetable WHERE *;")
    }
}
