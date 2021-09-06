mod database;
mod directory;
mod env;
mod migrations_table;
mod sql_file;
use clap::Parser;
use database::{DatabaseClient, PostgresClient, PostgresConfiguration};
use directory::{Directory, DirectoryError};
use env::Env;
use migrations_table::MigrationsTable;
use sql_file::{SQLFile, SQLFileError};
use std::error::Error;
use std::fmt;
use std::fmt::Display;

#[macro_use]
extern crate quick_error;
quick_error! {
    #[derive(Debug)]
    pub enum ApplicationError {
        SanityError(err:SanityError) {
            source(err)
            display("Sanity Error: {}", err)
            from()
        }
        SQLError(err: SQLFileError) {
            source(err)
            display("SQL Error: {}", err)
            from()
        }
        DirectoryError(err: DirectoryError) {
            source(err)
                display("Directory Error: {}", err)
                from()
        }
        PostgresError(err: postgres::Error) {
            source(err)
                display("Postgres Error: {}", err)
                from()
        }
    }
}

pub type ApplicationResult<T> = Result<T, ApplicationError>;

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
struct AppContext {
    pub migration_table: MigrationsTable,
    pub database_client: Box<dyn DatabaseClient>,
    pub sql_sets: Vec<SQLFile>,
}

#[derive(Debug)]
struct MigrateAppArguments {
    up_key: String,
}

#[derive(Debug)]
struct SanityAppArguments {
    ignore_new_files: bool,
}

struct Osprey {}
impl Osprey {
    pub fn migrate(
        app_context: &mut AppContext,
        app_arguments: &MigrateAppArguments,
    ) -> ApplicationResult<()> {
        // grab previous migrations with up tag
        let migration_instances = app_context.migration_table.get_migrations_by_tag(
            app_arguments.up_key.as_str(),
            &mut *app_context.database_client,
        )?;

        let mut executed_query_sets = 0;
        let mut executed_queries = 0;

        for file in app_context.sql_sets.iter() {
            // see if this file has a query set with the given tag
            if let Some(up_query) = file.query_hash_map.get(app_arguments.up_key.as_str()) {
                // see if this migration set has already happened
                if migration_instances
                    .iter()
                    .find(|x| x.name == file.name)
                    .is_some()
                {
                    continue;
                }

                // execute all queries in the set with given tag
                for query in up_query.queries.iter() {
                    app_context.database_client.batch_execute(query)?;
                    executed_queries = executed_queries + 1;
                }

                executed_query_sets = executed_query_sets + 1;

                // record migration
                app_context.migration_table.add_migration(
                    &mut *app_context.database_client,
                    up_query.hash.as_str(),
                    file.name.as_str(),
                    app_arguments.up_key.as_str(),
                )?;
            }
        }

        println!(
            "Executed {} query sets with {} total queries",
            executed_query_sets, executed_queries
        );

        Ok(())
    }

    fn sanity(
        app_context: &mut AppContext,
        app_arguments: &SanityAppArguments,
    ) -> ApplicationResult<()> {
        let migration_instances = app_context
            .migration_table
            .get_migrations(&mut *app_context.database_client)?;

        for file in app_context.sql_sets.iter() {
            let filtered = migration_instances
                .iter()
                .filter(|x| x.name == file.name)
                .into_iter();
            let mut count = 0;

            // see if this migration set has already happened
            for migration in filtered {
                // check if this file still has the tagged query used in this migration instance
                if let Some(tag_query_set) = file.query_hash_map.get(migration.tag.as_str()) {
                    // see if the query set is unchanged since the last migration
                    if tag_query_set.hash != migration.hash {
                        return Err(ApplicationError::SanityError(
                            SanityError::FileQuerySetChanged(
                                file.name.clone(),
                                migration.tag.clone(),
                            ),
                        ));
                    }
                } else {
                    // this file doesn't have the tagged query, return error
                    return Err(ApplicationError::SanityError(
                        SanityError::FileNoContainTag(file.name.clone(), migration.tag.clone()),
                    ));
                }
                count += 1;
            }

            if !app_arguments.ignore_new_files && count == 0 {
                return Err(ApplicationError::SanityError(SanityError::FileNotMigrated(
                    file.name.clone(),
                )));
            }
        }
        Ok(())
    }
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, default_value = "_migrations")]
    migrations_directory: String,
    #[clap(short = 't', long, default_value = "./migrations/")]
    migrations_table: String,
    #[clap(short = 'g', long, default_value = "up")]
    tag: String,
    #[clap(short = 'r', long, default_value = "migrate")]
    run: String,
    #[clap(short = 'i', long)]
    ignore_new_files: bool,
}

fn main() -> ApplicationResult<()> {
    let args = Args::parse();

    // get postgres info from environment variables
    let dbhost = Env::get_value_or_default("POSTGRES_HOST", "localhost");
    let password = Env::get_value_or_default("POSTGRES_PASSWORD", "postgres");
    let username = Env::get_value_or_default("POSTGRES_USER", "postgres");
    let db_name = Env::get_value_or_default("POSTGRES_DB", "postgres");

    // read all .sql files in the directory, parse them
    let directory_files =
        Directory::new(args.migrations_directory.as_str())?.get_file_list("sql")?;
    let mut all_query_sets = vec![];
    for file in directory_files {
        let f = SQLFile::new_from_file(&file)?;
        all_query_sets.push(f);
    }

    let postgres_configuration = PostgresConfiguration::new()
        .host(dbhost)
        .username(username)
        .password(password)
        .database_name(db_name);

    let mut dbclient = PostgresClient::new(&postgres_configuration)?.boxed();

    let migrations_table = MigrationsTable::new(args.migrations_table.as_str(), &mut *dbclient)?;

    let mut app_context = AppContext {
        migration_table: migrations_table,
        database_client: dbclient,
        sql_sets: all_query_sets,
    };

    match args.run.as_str() {
        "migrate" => {
            let app_arguments = MigrateAppArguments { up_key: args.tag };
            Osprey::migrate(&mut app_context, &app_arguments)?;
        }
        "sanity" => {
            let app_arguments = SanityAppArguments {
                ignore_new_files: args.ignore_new_files,
            };
            Osprey::sanity(&mut app_context, &app_arguments)?;
        }
        _ => println!("Unrecognized Command"),
    }

    Ok(())
}
