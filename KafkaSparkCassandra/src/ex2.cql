CREATE TABLE a20444878.Music ( 
	artistName text,
	albumName text,
	numberSold int,
	cost int,
	PRIMARY KEY(artistName, albumName)
) WITH CLUSTERING ORDER BY (albumName DESC);
