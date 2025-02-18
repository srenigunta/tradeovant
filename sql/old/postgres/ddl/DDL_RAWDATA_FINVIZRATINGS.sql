create table rawdata.finvizratings

(
	symbol varchar(20) not null,
	ratingdate date not null,
	ratingtype varchar(50) not null,
	ratingby varchar(100) not null,
	ratingvalue varchar(200) not null,
	pricetarget varchar(50),
	insertdate date
)
tablespace rawspace;

commit;