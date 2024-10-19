CREATE KEYSPACE IF NOT EXISTS csharpdriver WITH REPLICATION = {
    'class' : 'NetworkTopologyStrategy',
    'datacenter1' : 3
    };

USE csharpdriver;

CREATE TYPE friends
    (
        friend_id     INT,
        friends_since TIMESTAMP
    );

CREATE TABLE IF NOT EXISTS person
(
    name       TEXT,
    user_id    INT,
    created_at DATE,
    ip_addr    INET,
    friends LIST<FROZEN<csharpdriver.friends>>,
    houses  MAP<TEXT, TEXT>,
    PRIMARY KEY (name, user_id)
);

// The ip addresses are randomly created by some website I found - Instellate
INSERT INTO person(name, user_id, created_at, ip_addr, friends, houses)
VALUES ('Instellate', 0, '2023-07-21', '178.163.120.7',
        [{ friend_id: 1, friends_since: '2024-09-13 12:43:56'}],
        { 'Riksgatan 1, 100 12 Stockholm' : 'The Swedish Parliament House' });

INSERT INTO person(name, user_id, created_at, ip_addr, friends, houses)
VALUES ('Flaze', 1, '2024-08-27', 'd060:f059:03e8:3bbf:7f7f:d83f:d782:26ba',
        [{ friend_id: 0, friends_since: '2024-09-13 12:43:56'}],
        { '1600 Pennsylvania Avenue NW, Washington, DC 20500, United States' : 'The white house' });
