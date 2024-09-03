CREATE KEYSPACE IF NOT EXISTS csharpdriver WITH REPLICATION = {
    'class' : 'NetworkTopologyStrategy',
    'datacenter1' : 3
    };

USE csharpdriver;

CREATE TABLE IF NOT EXISTS person
(
    name       TEXT,
    user_id    INT,
    created_at DATE,
    ip_addr    INET,
    PRIMARY KEY (name, user_id)
);

// The ip addresses are randomly created by some website I found - Instellate
INSERT INTO person(name, user_id, created_at, ip_addr)
VALUES ('Instellate', 0, '2023-07-21', '178.163.120.7');

INSERT INTO person(name, user_id, created_at, ip_addr)
VALUES ('Flaze', 1, '2024-08-27', 'd060:f059:03e8:3bbf:7f7f:d83f:d782:26ba');
