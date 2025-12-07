package server

import (
	"context"
	"time"

	vyletdatabase "github.com/vylet-app/go/database/proto"
	"github.com/vylet-app/go/internal/helpers"
)

func (s *Server) CreateProfile(ctx context.Context, req *vyletdatabase.CreateProfileRequest) (*vyletdatabase.CreateProfileResponse, error) {
	logger := s.logger.With("name", "CreateProfile")

	var createdAt string
	if req.CreatedAt != nil {
		createdAt = *req.CreatedAt
	}

	now := time.Now().UTC()

	createdAtTime, err := time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		createdAtTime = now
	}

	if err := s.cqlSession.Query(
		`
		INSERT INTO profiles
			(did, created_at, indexed_at, updated_at)
		VALUES
			(?, ?, ?, ?)
		`, req.Did, createdAtTime, now, now,
	).WithContext(ctx).Exec(); err != nil {
		logger.Error("failed to create profile", "did", req.Did, "err", err)
		return &vyletdatabase.CreateProfileResponse{
			Error: helpers.ToStringPtr(err.Error()),
		}, nil
	}

	return &vyletdatabase.CreateProfileResponse{}, nil
}

func (s *Server) GetProfile(ctx context.Context, req *vyletdatabase.GetProfileRequest) (*vyletdatabase.GetProfileResponse, error) {
	return nil, nil
}
