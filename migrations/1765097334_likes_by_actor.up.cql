CREATE TABLE IF NOT EXISTS likes_by_actor (
	post_uri TEXT,
	author_did TEXT,
	created_at TIMESTAMP,
	indexed_at TIMESTAMP,
	PRIMARY KEY (author_did, created_at, post_uri),
) WITH CLUSTERING ORDER BY (created_at DESC, post_uri ASC);
