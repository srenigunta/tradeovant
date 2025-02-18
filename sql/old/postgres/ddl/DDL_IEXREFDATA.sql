create table rawdata.iexsymbols
(
	symbolsid int not null,
	symbol varchar(10) not null,
	exchange varchar(20) not null,
	exchangesuffix varchar(10),
	exchangename varchar(500),
	name varchar(1000),
	date date,
	type varchar(10),
	iexid varchar(50),
	region varchar(10),
	currency varchar(10),
	isenabled varchar(10),
	figi varchar(50),
	cik int,
	lei varchar(500)
)
tablespace dataspaces;

alter table rawdata.iexsymbols add constraint symbols_pk primary key (symbolsid);

create index symbol_idx1 on rawdata.iexsymbols(symbol) tablespace indexspace;
create index symbol_idx2 on rawdata.iexsymbols(exchange) tablespace indexspace;
create index symbol_idx3 on rawdata.iexsymbols(cik) tablespace indexspace;

commit;

create table rawdata.iexexchanges
(
	exchangesid int not null,
	mic varchar(10) not null,
	name varchar(500) not null,
	longName varchar(1000),
	tapeId varchar(10),
	oatsId varchar(10),
	refId varchar(10),
	type varchar(40)
)
tablespace dataspaces;

alter table rawdata.iexexchanges add constraint exchanges_pk primary key (exchangesid);

create index exchanges_idx1 on rawdata.iexexchanges(mic) tablespace indexspace;
create index exchanges_idx2 on rawdata.iexexchanges(name) tablespace indexspace;

commit;


create table rawdata.iextags
(
	tagsid int not null,
	name varchar(1000) not null
)
tablespace dataspaces;

create index tags_idx1 on rawdata.iextags(name) tablespace indexspace;

commit;


create table rawdata.iexsectors
(
	sectorsid int not null,
	name varchar(1000) not null
)
tablespace dataspaces;

create index sectors_idx1 on rawdata.iexsectors(name) tablespace indexspace;

create table rawdata.iexcompany
(
	symbolid int not null,
	symbol varchar(10) not null,
	companyname varchar(200),
	employees varchar(40),
	exchange varchar(200),
	industry varchar(200),
	website varchar(200),
	description varchar(4000),
	ceo varchar(200),
	securityname varchar(200),
	issuetype varchar(200),
	sector varchar(200),
	primarysiccode varchar(200),
	tags text[],
	address varchar(200),
	address2 varchar(200),
	state varchar(200),
	city varchar(200),
	zip varchar(200),
	country varchar(200),
	phone varchar(200)
)
tablespace dataspaces;

create index company_idx1 on rawdata.iexcompany(symbol) tablespace indexspace;

commit;
