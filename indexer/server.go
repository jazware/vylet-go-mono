package indexer

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bluesky-social/go-util/pkg/bus/consumer"
	vyletkafka "github.com/vylet-app/go/bus/proto"
)

type Server struct {
	logger *slog.Logger

	consumer *consumer.Consumer[*vyletkafka.FirehoseEvent]
}

type Args struct {
	Logger *slog.Logger

	BootstrapServers []string
	InputTopic       string
	ConsumerGroup    string
}

func New(args *Args) (*Server, error) {
	if args.Logger == nil {
		args.Logger = slog.Default()
	}

	logger := args.Logger

	busConsumer, err := consumer.New(
		logger.With("component", "consumer"),
		args.BootstrapServers,
		args.InputTopic,
		args.ConsumerGroup,
		consumer.WithOffset[*vyletkafka.FirehoseEvent](consumer.OffsetStart),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create new consumer: %w", err)
	}

	server := Server{
		logger: logger,

		consumer: busConsumer,
	}

	return &server, nil
}

func (s *Server) Run(ctx context.Context) error {
	logger := s.logger.With("name", "Run")

	shutdownConsumer := make(chan struct{}, 1)
	consumerShutdown := make(chan struct{}, 1)

	go func() {

	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-signals:
		logger.Info("received exit signal", "signal", sig)
		close(shutdownConsumer)
	case <-ctx.Done():
		logger.Info("context cancelled")
		close(shutdownConsumer)
	case <-consumerShutdown:
		logger.Warn("consumer shut down unexpectedly")
	}

	_, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s.consumer.Close()

	return nil
}
