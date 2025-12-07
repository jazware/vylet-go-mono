package server

import (
	"fmt"
	"log"

	"github.com/gocql/gocql"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/cassandra"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

// RunMigrations applies database migrations
func RunMigrations(session *gocql.Session, migrationsPath string) error {
	driver, err := cassandra.WithInstance(session, &cassandra.Config{
		KeyspaceName: session.Query("").Consistency(gocql.One).GetConsistency().String(),
	})
	if err != nil {
		return fmt.Errorf("failed to create migration driver: %w", err)
	}

	m, err := migrate.NewWithDatabaseInstance(
		fmt.Sprintf("file://%s", migrationsPath),
		"cassandra",
		driver,
	)
	if err != nil {
		return fmt.Errorf("failed to create migrate instance: %w", err)
	}

	// Get current version
	version, dirty, err := m.Version()
	if err != nil && err != migrate.ErrNilVersion {
		return fmt.Errorf("failed to get migration version: %w", err)
	}

	if dirty {
		log.Printf("WARNING: Database is in dirty state at version %d", version)
		return fmt.Errorf("database is in dirty state, manual intervention required")
	}

	log.Printf("Current migration version: %d", version)

	// Run migrations
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("failed to run migrations: %w", err)
	}

	newVersion, _, _ := m.Version()
	log.Printf("Migrations complete. Current version: %d", newVersion)

	return nil
}

// RollbackMigration rolls back the last migration
func RollbackMigration(session *gocql.Session, migrationsPath string) error {
	driver, err := cassandra.WithInstance(session, &cassandra.Config{
		KeyspaceName: session.Query("").Consistency(gocql.One).GetConsistency().String(),
	})
	if err != nil {
		return fmt.Errorf("failed to create migration driver: %w", err)
	}

	m, err := migrate.NewWithDatabaseInstance(
		fmt.Sprintf("file://%s", migrationsPath),
		"cassandra",
		driver,
	)
	if err != nil {
		return fmt.Errorf("failed to create migrate instance: %w", err)
	}

	if err := m.Steps(-1); err != nil {
		return fmt.Errorf("failed to rollback migration: %w", err)
	}

	version, _, _ := m.Version()
	log.Printf("Rolled back to version: %d", version)

	return nil
}
