CREATE TABLE IF NOT EXISTS likes_by_post (
	post_uri TEXT,
	author_did TEXT,
	created_at TIMESTAMP,
	indexed_at TIMESTAMP,
	PRIMARY KEY (post_uri, created_at, author_did),
) WITH CLUSTERING ORDER BY (created_at DESC, author_did ASC);
