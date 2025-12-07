package indexer

import (
	"context"
	"encoding/json"
	"fmt"

	vyletkafka "github.com/vylet-app/go/bus/proto"
	vyletdatabase "github.com/vylet-app/go/database/proto"
	"github.com/vylet-app/go/generated/vylet"
)

func (s *Server) handleActorProfile(ctx context.Context, evt *vyletkafka.FirehoseEvent) error {
	var rec vylet.ActorProfile
	op := evt.Commit

	if op.Operation == vyletkafka.CommitOperation_COMMIT_OPERATION_CREATE {
		if err := json.Unmarshal(op.Record, &rec); err != nil {
			return fmt.Errorf("failed to unmarshal profile record: %w", err)
		}

		resp, err := s.db.Profile.CreateProfile(ctx, &vyletdatabase.CreateProfileRequest{
			Did:       evt.Did,
			CreatedAt: rec.CreatedAt,
		})
		if err != nil {
			return fmt.Errorf("failed to create create profile request: %w", err)
		}
		if resp.Error != nil {
			return fmt.Errorf("error creating profile: %s", *resp.Error)
		}
	} else {
		return fmt.Errorf("unhandled operation")
	}

	return nil
}
