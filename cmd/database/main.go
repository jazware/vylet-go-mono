package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/bluesky-social/go-util/pkg/telemetry"
	"github.com/urfave/cli/v2"
	"github.com/vylet-app/go/database/server"
)

func main() {
	app := cli.App{
		Name: "vylet-database",
		Flags: []cli.Flag{
			telemetry.CLIFlagDebug,
			telemetry.CLIFlagMetricsListenAddress,
			&cli.StringFlag{
				Name:    "listen-addr",
				Value:   ":9090",
				EnvVars: []string{"VYLET_DATABASE_LISTEN_ADDR"},
			},
			&cli.StringSliceFlag{
				Name:    "cassandra-addrs",
				Value:   cli.NewStringSlice("127.0.0.1"),
				EnvVars: []string{"VYLET_DATABASE_CASSANDRA_ADDRS"},
			},
			&cli.StringFlag{
				Name:    "cassandra-keyspace",
				Value:   "vylet",
				EnvVars: []string{"VYLET_DATABASE_CASSANDRA_KEYSPACE"},
			},
		},
		Action: run,
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func run(cmd *cli.Context) error {
	ctx := context.Background()

	logger := telemetry.StartLogger(cmd)
	telemetry.StartMetrics(cmd)

	server, err := server.New(&server.Args{
		Logger: logger,

		ListenAddr:        cmd.String("listen-addr"),
		CassandraAddrs:    cmd.StringSlice("cassandra-addrs"),
		CassandraKeyspace: cmd.String("cassandra-keyspace"),
	})
	if err != nil {
		return fmt.Errorf("failed to create new server: %w", err)
	}

	if err := server.Run(ctx); err != nil {
		return fmt.Errorf("failed to run server: %w", err)
	}

	return nil
}
