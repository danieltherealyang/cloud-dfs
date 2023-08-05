DROP DATABASE IF EXISTS fsmap;
CREATE DATABASE fsmap;
USE fsmap;

-- CREATE TABLE IF NOT EXISTS file_to_node(
--   `filename` VARCHAR(255) PRIMARY KEY,
--   `node` VARCHAR(255)
-- );

CREATE TABLE IF NOT EXISTS file_to_metadata(
  `filename` VARCHAR(255) PRIMARY KEY,
  `metadata` BINARY(4) UNIQUE
);

INSERT INTO file_to_metadata VALUES ("Test", "1234");
INSERT INTO file_to_metadata VALUES ("Test2", "5432");