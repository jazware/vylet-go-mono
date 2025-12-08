package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/gocql/gocql"
	vyletdatabase "github.com/vylet-app/go/database/proto"
	"github.com/vylet-app/go/internal/helpers"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Server) getPostImages(ctx context.Context, postUri string) ([]*vyletdatabase.Image, error) {
	query := `
		SELECT image_index, cid, alt, width, height, size, mime
		FROM images_by_post
		WHERE post_uri = ?
		ORDER BY image_index ASC
	`

	iter := s.cqlSession.Query(query, postUri).WithContext(ctx).Iter()
	defer iter.Close()

	var images []*vyletdatabase.Image

	for {
		img := &vyletdatabase.Image{}
		var imageIndex int

		if !iter.Scan(
			&imageIndex,
			&img.Cid,
			&img.Alt,
			&img.Width,
			&img.Height,
			&img.Size,
			&img.Mime,
		) {
			break
		}

		images = append(images, img)
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to iterate images: %w", err)
	}

	return images, nil
}

func (s *Server) CreatePost(ctx context.Context, req *vyletdatabase.CreatePostRequest) (*vyletdatabase.CreatePostResponse, error) {
	logger := s.logger.With("name", "CreatePost")

	aturi, err := syntax.ParseATURI(req.Post.Uri)
	if err != nil {
		return nil, fmt.Errorf("failed to parse aturi: %w", err)
	}

	did := aturi.Authority().String()
	now := time.Now().UTC()

	batch := s.cqlSession.NewBatch(gocql.LoggedBatch).WithContext(ctx)

	postArgs := []any{
		req.Post.Uri,
		req.Post.Cid,
		did,
		req.Post.Caption,
		req.Post.Facets,
		req.Post.CreatedAt.AsTime(),
		now,
	}

	postQuery := `
		INSERT INTO %s
			(uri, cid, author_did, caption, facets, created_at, indexed_at)
		VALUES
			(?, ?, ?, ?, ?, ?, ?)
	`

	batch.Query(fmt.Sprintf(postQuery, "posts_by_uri"), postArgs...)
	batch.Query(fmt.Sprintf(postQuery, "posts_by_actor"), postArgs...)

	for idx, img := range req.Post.Images {
		batch.Query(
			`INSERT INTO images_by_post
				(post_uri, image_index, cid, alt, width, height, size, mime)
			VALUES
				(?, ?, ?, ?, ?, ?, ?, ?)`,
			req.Post.Uri,
			idx,
			img.Cid,
			img.Alt,
			img.Width,
			img.Height,
			img.Size,
			img.Mime,
		)
	}

	if err := s.cqlSession.ExecuteBatch(batch); err != nil {
		logger.Error("failed to create post", "uri", req.Post.Uri, "err", err)
		return &vyletdatabase.CreatePostResponse{
			Error: helpers.ToStringPtr(err.Error()),
		}, nil
	}

	return &vyletdatabase.CreatePostResponse{}, nil
}

func (s *Server) DeletePost(ctx context.Context, req *vyletdatabase.DeletePostRequest) (*vyletdatabase.DeletePostResponse, error) {
	logger := s.logger.With("name", "DeletePost", "uri", req.Uri)

	aturi, err := syntax.ParseATURI(req.Uri)
	if err != nil {
		return nil, fmt.Errorf("failed to parse aturi: %w", err)
	}
	did := aturi.Authority().String()

	var createdAt time.Time
	query := `
		SELECT created_at
		FROM posts_by_uri
		WHERE uri = ?
	`
	if err := s.cqlSession.Query(query, req.Uri).WithContext(ctx).Scan(&createdAt); err != nil {
		if err == gocql.ErrNotFound {
			logger.Warn("post not found", "uri", req.Uri)
			return &vyletdatabase.DeletePostResponse{
				Error: helpers.ToStringPtr("post not found"),
			}, nil
		}
		logger.Error("failed to fetch post", "uri", req.Uri, "err", err)
		return &vyletdatabase.DeletePostResponse{
			Error: helpers.ToStringPtr(err.Error()),
		}, nil
	}

	batch := s.cqlSession.NewBatch(gocql.LoggedBatch).WithContext(ctx)

	batch.Query(`
		DELETE FROM posts_by_uri
		WHERE uri = ?
	`, req.Uri)

	batch.Query(`
		DELETE FROM posts_by_actor
		WHERE author_did = ? AND created_at = ? AND uri = ?
	`, did, createdAt, req.Uri)

	batch.Query(`
		DELETE FROM images_by_post
		WHERE post_uri = ?
	`, req.Uri)

	if err := s.cqlSession.ExecuteBatch(batch); err != nil {
		logger.Error("failed to delete post", "uri", req.Uri, "err", err)
		return &vyletdatabase.DeletePostResponse{
			Error: helpers.ToStringPtr(err.Error()),
		}, nil
	}

	return &vyletdatabase.DeletePostResponse{}, nil
}

func (s *Server) GetPosts(ctx context.Context, req *vyletdatabase.GetPostsRequest) (*vyletdatabase.GetPostsResponse, error) {
	logger := s.logger.With("name", "GetPosts", "uris", req.Uris)

	if len(req.Uris) == 0 {
		return nil, fmt.Errorf("at least one URI must be specified")
	}

	query := `
		SELECT uri, cid, author_did, caption, facets, created_at, indexed_at
		FROM posts_by_uri
		WHERE uri IN ?
	`

	iter := s.cqlSession.Query(query, req.Uris).WithContext(ctx).Iter()
	defer iter.Close()

	posts := make(map[string]*vyletdatabase.Post)
	for {
		post := &vyletdatabase.Post{}
		var createdAt, indexedAt time.Time

		if !iter.Scan(
			&post.Uri,
			&post.Cid,
			&post.AuthorDid,
			&post.Caption,
			&post.Facets,
			&createdAt,
			&indexedAt,
		) {
			break
		}

		post.CreatedAt = timestamppb.New(createdAt)
		post.IndexedAt = timestamppb.New(indexedAt)

		images, err := s.getPostImages(ctx, post.Uri)
		if err != nil {
			logger.Warn("failed to fetch images for post", "uri", post.Uri, "err", err)
		} else {
			post.Images = images
		}

		posts[post.Uri] = post
	}

	if err := iter.Close(); err != nil {
		logger.Error("failed to iterate posts", "err", err)
		return &vyletdatabase.GetPostsResponse{
			Error: helpers.ToStringPtr(err.Error()),
		}, nil
	}

	return &vyletdatabase.GetPostsResponse{
		Posts: posts,
	}, nil
}

func (s *Server) GetPostsByActor(ctx context.Context, req *vyletdatabase.GetPostsByActorRequest) (*vyletdatabase.GetPostsByActorResponse, error) {
	logger := s.logger.With("name", "GetPostsByActor", "did", req.Did)

	if req.Limit <= 0 {
		return nil, fmt.Errorf("limit must be greater than 0")
	}

	var (
		query string
		args  []any
	)

	if req.Cursor != nil && *req.Cursor != "" {
		cursorParts := strings.SplitN(*req.Cursor, "|", 2)
		if len(cursorParts) != 2 {
			logger.Error("invalid cursor format", "cursor", *req.Cursor)
			return &vyletdatabase.GetPostsByActorResponse{
				Error: helpers.ToStringPtr("invalid cursor format"),
			}, nil
		}

		cursorTime, err := time.Parse(time.RFC3339Nano, cursorParts[0])
		if err != nil {
			logger.Error("failed to parse cursor timestamp", "cursor", *req.Cursor, "err", err)
			return &vyletdatabase.GetPostsByActorResponse{
				Error: helpers.ToStringPtr("invalid cursor format"),
			}, nil
		}
		cursorUri := cursorParts[1]

		query = `
			SELECT uri, cid, author_did, caption, facets, created_at, indexed_at
			FROM posts_by_actor
			WHERE author_did = ? AND (created_at, uri) < (?, ?)
			ORDER BY created_at DESC, uri ASC
			LIMIT ?
		`
		args = []any{req.Did, cursorTime, cursorUri, req.Limit + 1}
	} else {
		query = `
			SELECT uri, cid, author_did, caption, facets, created_at, indexed_at
			FROM posts_by_actor
			WHERE author_did = ?
			ORDER BY created_at DESC, uri ASC
			LIMIT ?
		`
		args = []any{req.Did, req.Limit + 1}
	}

	iter := s.cqlSession.Query(query, args...).WithContext(ctx).Iter()
	defer iter.Close()

	var postsList []*vyletdatabase.Post
	for {
		post := &vyletdatabase.Post{}
		var createdAt, indexedAt time.Time

		if !iter.Scan(
			&post.Uri,
			&post.Cid,
			&post.AuthorDid,
			&post.Caption,
			&post.Facets,
			&createdAt,
			&indexedAt,
		) {
			break
		}

		post.CreatedAt = timestamppb.New(createdAt)
		post.IndexedAt = timestamppb.New(indexedAt)
		postsList = append(postsList, post)
	}

	if err := iter.Close(); err != nil {
		logger.Error("failed to iterate posts", "err", err)
		return &vyletdatabase.GetPostsByActorResponse{
			Error: helpers.ToStringPtr(err.Error()),
		}, nil
	}

	var nextCursor *string
	if len(postsList) > int(req.Limit) {
		postsList = postsList[:req.Limit]
		lastPost := postsList[len(postsList)-1]
		cursorStr := fmt.Sprintf("%s|%s",
			lastPost.CreatedAt.AsTime().Format(time.RFC3339Nano),
			lastPost.Uri)
		nextCursor = &cursorStr
	}

	posts := make(map[string]*vyletdatabase.Post)
	for _, post := range postsList {
		images, err := s.getPostImages(ctx, post.Uri)
		if err != nil {
			logger.Warn("failed to fetch images for post", "uri", post.Uri, "err", err)
		} else {
			post.Images = images
		}
		posts[post.Uri] = post
	}

	return &vyletdatabase.GetPostsByActorResponse{
		Posts:  posts,
		Cursor: nextCursor,
	}, nil
}

func (s *Server) GetPostInteractionCounts(ctx context.Context, req *vyletdatabase.GetPostInteractionCountsRequest) (*vyletdatabase.GetPostInteractionCountsResponse, error) {
	logger := s.logger.With("name", "GetPostInteractionCounts", "uri", req.Uri)

	var likeCount, replyCount int64

	query := `
		SELECT like_count
		FROM post_interaction_counts
		WHERE post_uri = ?
	`

	err := s.cqlSession.Query(query, req.Uri).WithContext(ctx).Scan(&likeCount)
	if err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			return &vyletdatabase.GetPostInteractionCountsResponse{
				Counts: &vyletdatabase.PostInteractionCounts{
					Likes:   0,
					Replies: 0,
				},
			}, nil
		}
		logger.Error("failed to fetch interaction counts", "uri", req.Uri, "err", err)
		return &vyletdatabase.GetPostInteractionCountsResponse{
			Error: helpers.ToStringPtr(err.Error()),
		}, nil
	}

	return &vyletdatabase.GetPostInteractionCountsResponse{
		Counts: &vyletdatabase.PostInteractionCounts{
			Likes:   likeCount,
			Replies: replyCount,
		},
	}, nil
}

func (s *Server) GetPostsInteractionCounts(ctx context.Context, req *vyletdatabase.GetPostsInteractionCountsRequest) (*vyletdatabase.GetPostsInteractionCountsResponse, error) {
	logger := s.logger.With("name", "GetPostsInteractionCounts")

	query := `
		SELECT post_uri, like_count
		FROM post_interaction_counts
		WHERE post_uri IN ?
	`

	iter := s.cqlSession.Query(query, req.Uris).WithContext(ctx).Iter()
	defer iter.Close()

	counts := make(map[string]*vyletdatabase.PostInteractionCounts)

	var uri string
	var likeCount, replyCount int64
	for iter.Scan(&uri, &likeCount) {
		counts[uri] = &vyletdatabase.PostInteractionCounts{
			Likes:   likeCount,
			Replies: replyCount,
		}
	}

	if err := iter.Close(); err != nil {
		logger.Error("failed to iterate interaction counts", "err", err)
		return &vyletdatabase.GetPostsInteractionCountsResponse{
			Error: helpers.ToStringPtr(err.Error()),
		}, nil
	}

	for _, uri := range req.Uris {
		if _, exists := counts[uri]; !exists {
			counts[uri] = &vyletdatabase.PostInteractionCounts{
				Likes:   0,
				Replies: 0,
			}
		}
	}

	return &vyletdatabase.GetPostsInteractionCountsResponse{
		Counts: counts,
	}, nil
}
