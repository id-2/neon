use std::{collections::HashMap, fmt::Display};

use anyhow::{Context, Result};
use compute_api::spec::Database;
use postgres::{Client, NoTls};
use tracing::info;
use url::Url;

use crate::pg_helpers::get_existing_dbs;

pub(crate) enum Migration<'m> {
    Cluster(&'m str),
    PerDatabase(&'m str),
}

impl<'m> Display for Migration<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cluster(migration) | Self::PerDatabase(migration) => f.write_str(migration),
        }
    }
}

pub(crate) struct MigrationRunner<'m> {
    connstr: Url,
    cluster_client: Client,
    migrations: &'m [Migration<'m>],
}

impl<'m> MigrationRunner<'m> {
    pub fn new(mut connstr: Url, migrations: &'m [Migration<'m>]) -> Result<Self> {
        connstr
            .query_pairs_mut()
            .append_pair("application_name", "migrations");

        let cluster_client = Client::connect(connstr.as_str(), NoTls)?;

        Ok(Self {
            connstr,
            cluster_client,
            migrations,
        })
    }

    fn get_migration_id(&mut self) -> Result<i64> {
        let query = "SELECT id FROM neon_migration.migration_id";
        let row = self
            .cluster_client
            .query_one(query, &[])
            .context("run_migrations get migration_id")?;

        Ok(row.get::<&str, i64>("id"))
    }

    fn update_migration_id(&mut self) -> Result<()> {
        let setval = format!(
            "UPDATE neon_migration.migration_id SET id={}",
            self.migrations.len()
        );

        self.cluster_client
            .simple_query(&setval)
            .context("run_migrations update id")?;

        Ok(())
    }

    fn prepare_migrations(&mut self) -> Result<()> {
        let query = "CREATE SCHEMA IF NOT EXISTS neon_migration";
        self.cluster_client.simple_query(query)?;

        let query = "CREATE TABLE IF NOT EXISTS neon_migration.migration_id (key INT NOT NULL PRIMARY KEY, id bigint NOT NULL DEFAULT 0)";
        self.cluster_client.simple_query(query)?;

        let query = "INSERT INTO neon_migration.migration_id VALUES (0, 0) ON CONFLICT DO NOTHING";
        self.cluster_client.simple_query(query)?;

        let query = "ALTER SCHEMA neon_migration OWNER TO cloud_admin";
        self.cluster_client.simple_query(query)?;

        let query = "REVOKE ALL ON SCHEMA neon_migration FROM PUBLIC";
        self.cluster_client.simple_query(query)?;

        Ok(())
    }

    fn run_migration_with_client(
        client: &mut Client,
        migration_id: usize,
        migration: &str,
    ) -> Result<()> {
        if migration.starts_with("-- SKIP") {
            info!("Skipping migration id={}", migration_id);
        } else {
            info!("Running migration id={}:\n{}\n", migration_id, migration);
            client
                .simple_query(migration)
                .with_context(|| format!("run_migration current_migration={}", migration_id))?;
        }

        Ok(())
    }

    fn run_migration(&self, db: &str, migration_id: usize, migration: &str) -> Result<()> {
        let mut connstr = self.connstr.clone();
        connstr.set_path(db);
        connstr
            .query_pairs_mut()
            .append_pair("application_name", "migrations");

        let mut client = Client::connect(connstr.as_str(), NoTls)?;

        Self::run_migration_with_client(&mut client, migration_id, migration)
    }

    pub fn run_migrations(mut self) -> Result<()> {
        self.prepare_migrations()?;

        let mut current_migration: usize = self.get_migration_id()? as usize;
        let starting_migration_id = current_migration;

        let mut dbs: Option<HashMap<String, Database>> = None;
        if self
            .migrations
            .iter()
            .any(|m| matches!(m, Migration::PerDatabase(_)))
        {
            dbs = Some(get_existing_dbs(&mut self.cluster_client)?);
        }

        // A Postgres connection string will always have a path with 1 segment, the database name
        let admin_db = self.connstr.path_segments().unwrap().next().unwrap();

        self.cluster_client
            .simple_query("BEGIN")
            .context("run_migrations begin")?;

        while current_migration < self.migrations.len() {
            match &self.migrations[current_migration] {
                Migration::Cluster(migration) => Self::run_migration_with_client(
                    &mut self.cluster_client,
                    current_migration,
                    migration,
                )?,
                Migration::PerDatabase(migration) => {
                    // Iterate over all non-invalid databases (datconnectivity = -2)
                    for db in dbs.as_ref().unwrap().iter().filter(|d| !d.1.invalid) {
                        /* Once all the databases have ran the migration, then we can run it in the
                         * admin database to mark the migration as complete. See the run for the
                         * admin database outside this loop.
                         */
                        if db.0 == admin_db {
                            continue;
                        }

                        info!("db: {} {}", db.0, db.1.restrict_conn);
                        if db.1.restrict_conn {
                            self.cluster_client.simple_query(
                                format!("ALTER DATABASE \"{}\" WITH allow_connections true", db.0)
                                    .as_str(),
                            )?;
                        }

                        /* Do not early return, so that we can try to reset the connectability of
                         * the db.
                         */
                        let result = self.run_migration(db.0, current_migration, migration);

                        if db.1.restrict_conn {
                            self.cluster_client.simple_query(
                                format!("ALTER DATABASE \"{}\" WITH allow_connections false", db.0)
                                    .as_str(),
                            )?;
                        }

                        if result.is_err() {
                            let _ = self.cluster_client.simple_query("ABORT");
                            return result;
                        }
                    }

                    // We can reuse the client here instead of creating a new one
                    Self::run_migration_with_client(
                        &mut self.cluster_client,
                        current_migration,
                        migration,
                    )?;
                }
            }

            current_migration += 1;
        }

        self.update_migration_id()?;

        self.cluster_client
            .simple_query("COMMIT")
            .context("run_migrations commit")?;

        info!(
            "Ran {} migrations",
            (self.migrations.len() - starting_migration_id)
        );

        Ok(())
    }
}
