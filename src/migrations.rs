use crate::database::DatabaseClient;
use crate::error::OspreyError;

// MigrationInstance represents a migration record from the migration table
#[derive(Debug)]
pub struct MigrationInstance {
    pub index: i32,
    pub name: String,
    pub tag: String,
    pub hash: String,
}

impl MigrationInstance {
    pub fn new(index: i32, name: &str, tag: &str, hash: &str) -> MigrationInstance {
        MigrationInstance {
            index,
            name: name.to_string(),
            tag: tag.to_string(),
            hash: hash.to_string(),
        }
    }
}

pub trait MigrationRecordStorage {
    fn create_table(&mut self) -> Result<(), OspreyError>;
    fn execute_queries(&mut self, queries: &[String]) -> Result<(), OspreyError>;
    fn add_record(&mut self, name: &str, tag: &str, hash: &str) -> Result<(), OspreyError>;
    fn get_records_by_tag(&mut self, tag: &str) -> Result<Vec<MigrationInstance>, OspreyError>;
    fn get_all_records(&mut self) -> Result<Vec<MigrationInstance>, OspreyError>;
}

pub struct DatabaseMigrationRecordStorage<'a> {
    table_name: &'a str,
    database_client: &'a mut dyn DatabaseClient,
}

impl<'a> DatabaseMigrationRecordStorage<'a> {
    pub fn new(
        table_name: &'a str,
        database_client: &'a mut dyn DatabaseClient,
    ) -> DatabaseMigrationRecordStorage<'a> {
        DatabaseMigrationRecordStorage {
            table_name,
            database_client,
        }
    }
}

impl<'a> MigrationRecordStorage for DatabaseMigrationRecordStorage<'a> {
    fn create_table(&mut self) -> Result<(), OspreyError> {
        // attempt to create the migrations table, if it already exists then do nothing
        // and return ok
        let query = format!(
            "CREATE TABLE IF NOT EXISTS {} ( \
            index  SERIAL PRIMARY KEY, \
            name TEXT, \
            tag TEXT NOT NULL, \
            applied_date DATE NOT NULL DEFAULT CURRENT_DATE, \
            hash TEXT \
            );",
            self.table_name
        );

        self.database_client.batch_execute(&query)?;
        Ok(())
    }

    fn execute_queries(&mut self, queries: &[String]) -> Result<(), OspreyError> {
        for query in queries.iter() {
            self.database_client.batch_execute(query)?;
        }
        Ok(())
    }

    fn add_record(&mut self, name: &str, tag: &str, hash: &str) -> Result<(), OspreyError> {
        let query = format!(
            "INSERT INTO {} (name, hash, tag) \
            VALUES('{}', '{}', '{}');
            ",
            self.table_name, name, hash, tag
        );

        self.database_client.batch_execute(&query)?;

        Ok(())
    }

    fn get_records_by_tag(&mut self, tag: &str) -> Result<Vec<MigrationInstance>, OspreyError> {
        let query = format!(
            "SELECT index, name, tag, hash, FROM {} WHERE tag = '{}'",
            self.table_name, tag
        );

        let rows = self.database_client.query_row(&query)?;

        let instances = rows
            .iter()
            .map(|row| MigrationInstance::new(row.get(0), row.get(1), row.get(2), row.get(3)))
            .collect();

        Ok(instances)
    }

    fn get_all_records(&mut self) -> Result<Vec<MigrationInstance>, OspreyError> {
        let query = format!("SELECT index, name, tag, hash, FROM {}", self.table_name);

        let rows = self.database_client.query_row(&query)?;

        let instances = rows
            .iter()
            .map(|row| MigrationInstance::new(row.get(0), row.get(1), row.get(2), row.get(3)))
            .collect();

        Ok(instances)
    }
}

pub struct Migrations<'a> {
    record_storage: &'a mut dyn MigrationRecordStorage,
}

impl<'a> Migrations<'a> {
    pub fn new(
        record_storage: &'a mut dyn MigrationRecordStorage,
    ) -> Result<Migrations<'a>, OspreyError> {
        record_storage.create_table()?;
        Ok(Migrations { record_storage })
    }

    pub fn execute_queries(&mut self, queries: &[String]) -> Result<(), OspreyError> {
        self.record_storage.execute_queries(queries)?;
        Ok(())
    }

    pub fn add_migration(&mut self, hash: &str, name: &str, tag: &str) -> Result<(), OspreyError> {
        self.record_storage.add_record(name, tag, hash)
    }

    pub fn get_migrations_by_tag(
        &mut self,
        tag: &str,
    ) -> Result<Vec<MigrationInstance>, OspreyError> {
        self.record_storage.get_records_by_tag(tag)
    }

    pub fn get_migrations(&mut self) -> Result<Vec<MigrationInstance>, OspreyError> {
        self.record_storage.get_all_records()
    }
}
