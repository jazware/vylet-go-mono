package server

import (
	"context"
	"time"

	vyletdatabase "github.com/vylet-app/go/database/proto"
	"github.com/vylet-app/go/internal/helpers"
	"google.golang.org/protobuf/types/known/timestamppb"
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
			(did, display_name, description, pronouns, avatar, created_at, indexed_at, updated_at)
		VALUES
			(?, ?, ?, ?, ?, ?, ?, ?)
		`,
		req.Did,
		req.DisplayName,
		req.Description,
		req.Pronouns,
		req.Avatar,
		createdAtTime,
		now,
		now,
	).WithContext(ctx).Exec(); err != nil {
		logger.Error("failed to create profile", "did", req.Did, "err", err)
		return &vyletdatabase.CreateProfileResponse{
			Error: helpers.ToStringPtr(err.Error()),
		}, nil
	}

	return &vyletdatabase.CreateProfileResponse{}, nil
}

func (s *Server) GetProfile(ctx context.Context, req *vyletdatabase.GetProfileRequest) (*vyletdatabase.GetProfileResponse, error) {
	logger := s.logger.With("name", "GetProfile")

	resp := &vyletdatabase.GetProfileResponse{}
	var createdAt, indexedAt time.Time

	if err := s.cqlSession.Query(
		`SELECT
			did, display_name, description, pronouns, avatar, created_at, indexed_at
		FROM profiles
		WHERE did = ?
		`,
		req.Did,
	).WithContext(ctx).Scan(
		&resp.Did,
		&resp.DisplayName,
		&resp.Description,
		&resp.Pronouns,
		&resp.Avatar,
		&createdAt,
		&indexedAt,
	); err != nil {
		logger.Error("failed to get profile", "did", req.Did, "err", err)
		return &vyletdatabase.GetProfileResponse{
			Error: helpers.ToStringPtr(err.Error()),
		}, nil
	}

	resp.CreatedAt = timestamppb.New(createdAt)
	resp.IndexedAt = timestamppb.New(indexedAt)

	return resp, nil
}

func (s *Server) DeleteProfile(ctx context.Context, req *vyletdatabase.DeleteProfileRequest) (*vyletdatabase.DeleteProfileResponse, error) {
	logger := s.logger.With("name", "DeleteProfile")

	if err := s.cqlSession.Query(
		`
		DELETE FROM profiles WHERE did = ?
		`,
		req.Did,
	).WithContext(ctx).Exec(); err != nil {
		logger.Error("failed to delete profile", "did", req.Did, "err", err)
		return &vyletdatabase.DeleteProfileResponse{
			Error: helpers.ToStringPtr(err.Error()),
		}, nil
	}

	return &vyletdatabase.DeleteProfileResponse{}, nil
}
