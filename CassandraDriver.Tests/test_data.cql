CREATE KEYSPACE IF NOT EXISTS csharpdriver WITH REPLICATION = {
    'class' : 'SimpleStrategy',
    'replication_factor' : 1
    };

USE csharpdriver;

CREATE TABLE IF NOT EXISTS person
(
    name    TEXT,
    user_id INT,
    PRIMARY KEY (name, user_id)
);

INSERT INTO person(name, user_id)
VALUES ('Instellate', 0);

INSERT INTO person(name, user_id)
VALUES ('Flaze', 1);
