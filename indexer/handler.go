package indexer

import (
	"context"

	vyletkafka "github.com/vylet-app/go/bus/proto"
)

func (s *Server) handleEvent(ctx context.Context, evt *vyletkafka.FirehoseEvent) error {
	return nil
}
