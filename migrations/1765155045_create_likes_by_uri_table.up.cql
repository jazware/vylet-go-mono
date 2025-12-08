CREATE TABLE IF NOT EXISTS likes_by_uri (
	uri TEXT PRIMARY KEY,
	cid TEXT,
	subject_uri TEXT,
	subject_cid TEXT,
	author_did TEXT,
	created_at TIMESTAMP,
	indexed_at TIMESTAMP,
);
