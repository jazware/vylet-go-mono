CREATE TABLE IF NOT EXISTS profiles (
    did TEXT PRIMARY KEY,
    display_name TEXT,
    description TEXT,
    pronouns TEXT,
    avatar TEXT,
    created_at TIMESTAMP,
    indexed_at TIMESTAMP,
    updated_at TIMESTAMP,
);
