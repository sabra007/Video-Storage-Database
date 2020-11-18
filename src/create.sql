---TABLES---
DROP TABLE IF EXISTS User1 CASCADE;
DROP TABLE IF EXISTS Channel CASCADE;
DROP TABLE IF EXISTS Video CASCADE;
DROP TABLE IF EXISTS Own_by CASCADE;
DROP TABLE IF EXISTS video_storage CASCADE;

-------------
---DOMAINS---
-------------
CREATE DOMAIN _GENDER VARCHAR(1) CHECK (value IN ( 'F' , 'M' , 'O') );
------------
---TABLES---
------------
CREATE TABLE User1
(
	uid INT NOT NULL PRIMARY KEY,
	fname VARCHAR(32) NOT NULL,
	lname VARCHAR(32) NOT NULL,
	uname VARCHAR(32) NOT NULL UNIQUE,
	phone VARCHAR(13) NOT NULL,
	gender1 _GENDER NOT NULL,
	address VARCHAR(256) NOT NULL,
	email VARCHAR(50) NOT NULL,
	age INT NOT NULL,
	fav_cat VARCHAR(32),
	password VARCHAR(32) NOT NULL
);


 CREATE TABLE Channel
(
	cid INT NOT NULL,
	numSubs INT NOT NULL,
	numLikes INT NOT NULL,
	cname VARCHAR(32) NOT NULL,
	cage INT NOT NULL,
	uid INT NOT NULL,
	PRIMARY KEY (cid),
	FOREIGN KEY (uid) REFERENCES User1(uid)
);

CREATE TABLE Video
(
	vin INT NOT NULL,
	cid INT NOT NULL,
	uid INT NOT NULL,
	numLikes INT NOT NULL,
	numDislikes INT NOT NULL,
	numViews INT NOT NULL,
	description VARCHAR(5000),
	category VARCHAR(50),
	title VARCHAR(256) NOT NULL,
	publicationDate DATE NOT NULL,
	PRIMARY KEY (vin),
	FOREIGN KEY (cid) REFERENCES Channel(cid),
	FOREIGN KEY (uid) REFERENCES User1(uid)
);



CREATE TABLE Video_Storage
(
	vid VARCHAR(16) NOT NULL,
	vin INT NOT NULL,
	storageid VARCHAR(16) NOT NULL,
	PRIMARY KEY (vid),
	FOREIGN KEY (vin) REFERENCES Video(vin)
);

----------------------------
-- INSERT DATA STATEMENTS --
----------------------------



COPY User1 (
	uid,
	fname,
	lname,
	uname,
	phone,
	gender1,
	address,
	email,
	age,
	fav_cat,
	password
)
FROM 'user1.csv'
WITH DELIMITER ',';




COPY Channel (
	cid,
	numSubs,
	numLikes,
	cname,
	cage,
	uid
)
FROM 'channel.csv'
WITH DELIMITER ',';

COPY Video (
	vin,
	cid,
	uid,
	numLikes,
	numDislikes,
	numViews,
	description,
	category,
	title,
	publicationDate
)
FROM 'video.csv'
WITH DELIMITER ',';