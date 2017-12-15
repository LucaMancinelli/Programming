USE project;
DROP TABLE IF EXISTS FMA;

create table FMA (
track_id INT NOT NULL PRIMARY KEY, 
album_listens INT,
track_duration	INT,
track_genre_top VARCHAR (500));



