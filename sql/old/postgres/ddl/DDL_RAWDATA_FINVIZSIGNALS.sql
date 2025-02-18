create table rawdata.finvizsignals_tech

(
	ticker			varchar(20) not null,
	beta			varchar(20),
	atr				varchar(20),
	sma20			varchar(20),
	sma50			varchar(20),
	sma200			varchar(20),
	ftwhigh			varchar(20),
	ftwlow			varchar(20),
	rsi				varchar(20),
	price			varchar(20) not null,
	change			varchar(20),
	fromopen		varchar(20),
	gap				varchar(20),
	volume			varchar(50),
	screenername	varchar(50)
)
tablespace rawspace;

commit;

create table rawdata.finvizsignals_perf

(
	ticker			varchar(20) not null,
	perfweek		varchar(20),
	perfmonth		varchar(20),
	perfquart		varchar(20),
	perfhalf		varchar(20),
	perfyear		varchar(20),
	perfytd			varchar(20),
	volatilityw		varchar(20),
	volatilitym		varchar(20),
	recom			varchar(20),
	avgvolume		varchar(50),
	relvolume		varchar(20),
	price			varchar(20),
	change			varchar(20),
	volume			varchar(50),
	screenername	varchar(100)
)
tablespace rawspace;

commit;

create table rawdata.finvizsignals_basic

(
	ticker			varchar(20) not null,
	company		varchar(500),
	sector		varchar(500),
	industry		varchar(500),
	country		varchar(20),
	marketcap		varchar(50),
	peratio			varchar(20),
	price		varchar(50),
	change		varchar(50),
	volume			varchar(50)
)
tablespace rawspace;
commit;

CREATE INDEX finvizsignals_basic_idx1 ON rawdata.finvizsignals_basic (ticker)
tablespace indexspace;