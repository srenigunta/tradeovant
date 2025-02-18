create table rawdata.finvizsummary

(
	symbol varchar(20) not null,
	columnname varchar(200) not null,
	datavalue varchar(500),
	insertdate date
)
tablespace rawspace;

commit;