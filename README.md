# Osprey

A database migrator tool written in Rust inspired by [sql-migrate](https://github.com/rubenv/sql-migrate) 

## In Progress!

This project is currently in progress and testing is on going. Code coverage will grow as time passes.

## Features

* A CLI tool that's easily used in a container
* Currently only supports Postgres
* Ability to "tag" sets of queries in sql files, use osprey to run all of query sets of specific tag in order
	* This gives the ability to "rollback" a migration
* "Sanity" checks to check the current migration state and make sure sql files have not changed since.

## Command Line Help

```
USAGE:
    osprey [OPTIONS]

OPTIONS:
    -g, --tag <TAG>                                      [default: up]
    -h, --help                                           Print help information
    -i, --ignore-new-files
    -m, --migrations-directory <MIGRATIONS_DIRECTORY>    [default: ./migrations/]
    -r, --run <RUN>                                      [default: migrate]
    -t, --migrations-table <MIGRATIONS_TABLE>            [default: _migrations]
    -V, --version                                        Print version information
```

## Postres Configurations

Osprey will read postgres configuration information from the environment variables. These match the exact environment variables that Postgres expects.

```
POSTGRES_HOST
POSTGRES_PASSWORD
POSTGRES_USER
POSTGRES_DB
```

