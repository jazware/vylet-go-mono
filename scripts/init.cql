-- Create keyspace if it doesn't exist
CREATE KEYSPACE IF NOT EXISTS vylet
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

-- Use the keyspace
USE vylet;
