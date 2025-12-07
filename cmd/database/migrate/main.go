package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gocql/gocql"
	"github.com/urfave/cli/v2"
	"github.com/vylet-app/go/database/server"
)

func main() {
	app := &cli.App{
		Name:  "migrate",
		Usage: "Cassandra database migration tool",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "migrations",
				Aliases: []string{"m"},
				Value:   "./migrations",
				Usage:   "Path to migrations directory",
				EnvVars: []string{"VYLET_DATABASE_MIGRATIONS_PATH"},
			},
			&cli.StringSliceFlag{
				Name:    "cassandra-addrs",
				Value:   cli.NewStringSlice("127.0.0.1"),
				Usage:   "Comma-separated Cassandra hosts",
				EnvVars: []string{"VYLET_DATABASE_CASSANDRA_ADDRS", "VYLET_DATABASE_CASSANDRA_HOSTS"},
			},
			&cli.StringFlag{
				Name:    "cassandra-keyspace",
				Aliases: []string{"k"},
				Value:   "vylet",
				Usage:   "Cassandra keyspace",
				EnvVars: []string{"VYLET_DATABASE_CASSANDRA_KEYSPACE"},
			},
		},
		Commands: []*cli.Command{
			{
				Name:    "up",
				Aliases: []string{"u"},
				Usage:   "Run migrations",
				Action:  runMigrationsUp,
			},
			{
				Name:    "down",
				Aliases: []string{"d"},
				Usage:   "Rollback last migration",
				Action:  runMigrationsDown,
			},
		},
		Action: func(c *cli.Context) error {
			// Default action if no command specified
			return runMigrationsUp(c)
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func runMigrationsUp(c *cli.Context) error {
	session, err := connectCassandra(c)
	if err != nil {
		return err
	}
	defer session.Close()

	migrationsPath := c.String("migrations")
	log.Println("Running migrations...")
	if err := server.RunMigrations(session, migrationsPath); err != nil {
		log.Fatalf("Migration failed: %v", err)
	}
	log.Println("Migrations completed successfully")
	return nil
}

func runMigrationsDown(c *cli.Context) error {
	session, err := connectCassandra(c)
	if err != nil {
		return err
	}
	defer session.Close()

	migrationsPath := c.String("migrations")
	log.Println("Rolling back last migration...")
	if err := server.RollbackMigration(session, migrationsPath); err != nil {
		log.Fatalf("Rollback failed: %v", err)
	}
	log.Println("Rollback completed successfully")
	return nil
}

func connectCassandra(c *cli.Context) (*gocql.Session, error) {
	fmt.Println(c.StringSlice("cassandra-addrs"))
	cluster := gocql.NewCluster(c.StringSlice("cassandra-addrs")...)
	cluster.Keyspace = c.String("cassandra-keyspace")
	cluster.Consistency = gocql.Quorum
	cluster.ProtoVersion = 4
	cluster.ConnectTimeout = time.Second * 10
	cluster.Timeout = time.Second * 10

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to cassandra: %w", err)
	}

	return session, nil
}
