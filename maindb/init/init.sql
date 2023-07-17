DROP DATABASE IF EXISTS fsmap;
CREATE DATABASE fsmap;
USE fsmap;

CREATE TABLE IF NOT EXISTS file_to_node(
  `filename` VARCHAR(255),
  `node` VARCHAR(255)
);