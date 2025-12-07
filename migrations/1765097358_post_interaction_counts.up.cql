CREATE TABLE IF NOT EXISTS post_interaction_counts (
	post_uri TEXT PRIMARY KEY,
	like_count COUNTER,
);
