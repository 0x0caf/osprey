use crate::database::DatabaseClient;
use postgres::Error;

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

#[derive(Debug)]
pub struct MigrationsTable {
    table_name: String,
}

impl MigrationsTable {
    pub fn new(
        table_name: &str,
        client: &mut dyn DatabaseClient,
    ) -> Result<MigrationsTable, Error> {
        Self::create(table_name, client)?;
        Ok(MigrationsTable {
            table_name: String::from(table_name),
        })
    }

    fn create(table_name: &str, client: &mut dyn DatabaseClient) -> Result<(), Error> {
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
            table_name
        );

        client.batch_execute(query.as_str())?;

        Ok(())
    }

    pub fn add_migration(
        &self,
        client: &mut dyn DatabaseClient,
        hash: &str,
        name: &str,
        tag: &str,
    ) -> Result<(), Error> {
        let query = format!(
            "INSERT INTO {} (name, hash, tag) \
            VALUES('{}', '{}', '{}');
            ",
            self.table_name, name, hash, tag
        );

        client.batch_execute(query.as_str())?;

        Ok(())
    }

    pub fn get_migrations_by_tag(
        &self,
        tag: &str,
        client: &mut dyn DatabaseClient,
    ) -> Result<Vec<MigrationInstance>, Error> {
        let mut instances = vec![];
        let query = format!(
            "SELECT index, name, tag, hash, FROM {} WHERE tag = '{}'",
            self.table_name, tag
        );

        let results = client.query_row(query.as_str())?;

        for row in results {
            let index: i32 = row.get(0);
            let name: &str = row.get(1);
            let tag: &str = row.get(2);
            let hash: &str = row.get(3);

            instances.push(MigrationInstance::new(index, name, tag, hash));
        }
        Ok(instances)
    }

    pub fn get_migrations(
        &self,
        client: &mut dyn DatabaseClient,
    ) -> Result<Vec<MigrationInstance>, Error> {
        let mut instances = vec![];
        let query = format!("SELECT index, name, tag, hash, FROM {}", self.table_name);

        let results = client.query_row(query.as_str())?;

        for row in results {
            let index: i32 = row.get(0);
            let name: &str = row.get(1);
            let tag: &str = row.get(2);
            let hash: &str = row.get(3);

            instances.push(MigrationInstance::new(index, name, tag, hash));
        }

        Ok(instances)
    }
}
