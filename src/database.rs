use postgres::{Client, Error, NoTls, Row};

#[derive(Debug)]
pub struct PostgresConfiguration {
    host: String,
    username: String,
    password: String,
    database_name: String,
}

impl PostgresConfiguration {
    pub fn new() -> PostgresConfiguration {
        PostgresConfiguration {
            host: String::new(),
            username: String::new(),
            password: String::new(),
            database_name: String::new(),
        }
    }

    pub fn host(mut self, host: String) -> PostgresConfiguration {
        self.host = host;
        self
    }

    pub fn username(mut self, username: String) -> PostgresConfiguration {
        self.username = username;
        self
    }

    pub fn password(mut self, password: String) -> PostgresConfiguration {
        self.password = password;
        self
    }

    pub fn database_name(mut self, database_name: String) -> PostgresConfiguration {
        self.database_name = database_name;
        self
    }

    pub fn get_url(&self) -> String {
        format!(
            "postgresql://{}:{}@{}/{}",
            self.username, self.password, self.host, self.database_name
        )
    }
}

pub trait DatabaseClient {
    fn batch_execute(&mut self, query: &str) -> Result<(), Error>;
    fn query_row(&mut self, query: &str) -> Result<Vec<Row>, Error>;
}

pub struct PostgresClient {
    client: Client,
}

impl PostgresClient {
    pub fn new(config: &PostgresConfiguration) -> Result<PostgresClient, Error> {
        let client = Client::connect(config.get_url().as_str(), NoTls)?;
        Ok(PostgresClient { client })
    }

    pub fn boxed(self) -> Box<PostgresClient> {
        Box::new(self)
    }
}
impl DatabaseClient for PostgresClient {
    fn batch_execute(&mut self, query: &str) -> Result<(), Error> {
        self.client.batch_execute(query)
    }

    fn query_row(&mut self, query: &str) -> Result<Vec<Row>, Error> {
        self.client.query(query, &[])
    }
}
