mod database;
mod directory;
mod env;
mod error;
mod migrations;
mod sql_file;
use clap::Parser;
use database::{PostgresClient, PostgresConfiguration};
use directory::Directory;
use env::Env;
use error::{OspreyError, SanityError};
use migrations::{DatabaseMigrationRecordStorage, MigrationRecordStorage, Migrations};
use sql_file::SQLFile;

#[macro_use]
extern crate quick_error;

struct AppContext<'a> {
    pub record_storage: &'a mut dyn MigrationRecordStorage,
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
    ) -> Result<(), OspreyError> {
        let mut migrations = Migrations::new(app_context.record_storage)?;

        // grab previous migrations with up tag
        let migration_instances = migrations.get_migrations_by_tag(&app_arguments.up_key)?;

        let mut executed_query_sets = 0;
        let mut executed_queries = 0;

        for file in app_context.sql_sets.iter() {
            // see if this file has a query set with the given tag
            if let Some(up_query) = file.query_hash_map.get(&app_arguments.up_key) {
                // see if this migration set has already happened
                if migration_instances.iter().any(|x| x.name == file.name) {
                    continue;
                }

                // execute all queries in the set with given tag
                migrations.execute_queries(&up_query.queries)?;

                executed_queries += up_query.queries.len();
                executed_query_sets += 1;

                // record migration
                migrations.add_migration(&up_query.hash, &file.name, &app_arguments.up_key)?;
            }
        }

        println!(
            "Executed {} query sets with {} total queries",
            executed_query_sets, executed_queries
        );

        Ok(())
    }

    fn instance_file_check(
        migration_instances: &[migrations::MigrationInstance],
        sql_sets: &[SQLFile],
        ignore_new_files: bool,
    ) -> Result<(), SanityError> {
        for file in sql_sets.iter() {
            let filtered = migration_instances.iter().filter(|x| x.name == file.name);
            let mut count = 0;

            // see if this migration set has already happened
            for migration in filtered {
                // check if this file still has the tagged query used in this migration instance
                let maybe_query_set = file.query_hash_map.get(&migration.tag);

                // this file doesn't have the tagged query, return error
                if maybe_query_set.is_none() {
                    return Err(SanityError::NoContainTag(
                        file.name.clone(),
                        migration.tag.clone(),
                    ));
                }
                // see if the query set is unchanged since the last migration
                if maybe_query_set.unwrap().hash != migration.hash {
                    return Err(SanityError::QuerySetChanged(
                        file.name.clone(),
                        migration.tag.clone(),
                    ));
                }
                count += 1;
            }

            if !ignore_new_files && count == 0 {
                return Err(SanityError::NotMigrated(file.name.clone()));
            }
        }

        for instance in migration_instances {
            let mut found = false;

            for sql_file in sql_sets.iter() {
                if sql_file.name == instance.name {
                    found = true;
                    break;
                }
            }
            if !found {
                return Err(SanityError::NoExist(instance.name.clone()));
            }
        }
        Ok(())
    }

    fn sanity(
        app_context: &mut AppContext,
        app_arguments: &SanityAppArguments,
    ) -> Result<(), OspreyError> {
        let mut migrations = Migrations::new(app_context.record_storage)?;
        let migration_instances = migrations.get_migrations()?;

        Self::instance_file_check(
            &migration_instances,
            &app_context.sql_sets,
            app_arguments.ignore_new_files,
        )?;
        Ok(())
    }
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, default_value = "./migrations/")]
    migrations_directory: String,
    #[clap(short = 't', long, default_value = "_migrations")]
    migrations_table: String,
    #[clap(short = 'a', long, default_value = "up")]
    tag: String,
    #[clap(short = 'r', long, default_value = "sanity")]
    run: String,
    #[clap(short = 'i', long)]
    ignore_new_files: bool,
}

fn main() -> Result<(), OspreyError> {
    let args = Args::parse();

    // get postgres info from environment variables
    let dbhost = Env::get_value_or_default("POSTGRES_HOST", "localhost");
    let password = Env::get_value_or_default("POSTGRES_PASSWORD", "postgres");
    let username = Env::get_value_or_default("POSTGRES_USER", "postgres");
    let db_name = Env::get_value_or_default("POSTGRES_DB", "postgres");

    // read all .sql files in the directory, parse them
    let directory_files = Directory::new(&args.migrations_directory)?.get_file_list("sql")?;
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

    let mut dbclient = PostgresClient::new(&postgres_configuration)?;
    let mut db_record_storage =
        DatabaseMigrationRecordStorage::new(&args.migrations_table, &mut dbclient);

    let mut app_context = AppContext {
        record_storage: &mut db_record_storage,
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
