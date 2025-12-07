package indexer

import (
	"context"
	"encoding/json"
	"fmt"

	vyletkafka "github.com/vylet-app/go/bus/proto"
	vyletdatabase "github.com/vylet-app/go/database/proto"
	"github.com/vylet-app/go/generated/vylet"
	"github.com/vylet-app/go/internal/helpers"
)

func (s *Server) handleActorProfile(ctx context.Context, evt *vyletkafka.FirehoseEvent) error {
	var rec vylet.ActorProfile
	op := evt.Commit

	switch op.Operation {
	case vyletkafka.CommitOperation_COMMIT_OPERATION_CREATE:
		if err := json.Unmarshal(op.Record, &rec); err != nil {
			return fmt.Errorf("failed to unmarshal profile record: %w", err)
		}

		req := vyletdatabase.CreateProfileRequest{
			Did:         evt.Did,
			DisplayName: rec.DisplayName,
			Description: rec.Description,
			Pronouns:    rec.Pronouns,
			CreatedAt:   rec.CreatedAt,
		}

		if rec.Avatar != nil {
			req.Avatar = helpers.ToStringPtr(rec.Avatar.Ref.String())
		}

		resp, err := s.db.Profile.CreateProfile(ctx, &req)
		if err != nil {
			return fmt.Errorf("failed to create create profile request: %w", err)
		}
		if resp.Error != nil {
			return fmt.Errorf("error creating profile: %s", *resp.Error)
		}
	case vyletkafka.CommitOperation_COMMIT_OPERATION_UPDATE:
		return fmt.Errorf("unhandled operation")
	case vyletkafka.CommitOperation_COMMIT_OPERATION_DELETE:
		resp, err := s.db.Profile.DeleteProfile(ctx, &vyletdatabase.DeleteProfileRequest{
			Did: evt.Did,
		})
		if err != nil {
			return fmt.Errorf("failed to create delete profile request: %w", err)
		}
		if resp.Error != nil {
			return fmt.Errorf("error deleting profile: %s", *resp.Error)
		}
	}

	return nil
}
