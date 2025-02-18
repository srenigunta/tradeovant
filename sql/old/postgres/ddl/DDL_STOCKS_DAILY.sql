create table stockdata.stockdaily
(
	stockdaily_skey int not null,
	symbol varchar(10) not null,
	tradedate date not null,
	open numeric(10, 2) not null,
	high numeric(10, 2) not null,
	low numeric(10, 2) not null,
	close numeric(10, 2) not null,
	adjclose numeric(10, 2) not null,
	volume int not null
)
tablespace dataspaces;

alter table stockdata.stockdaily add constraint stockdaily_pk primary key (stockdaily_skey);

create index stockdaily_idx1 on stockdata.stockdaily(symbol) tablespace indexspace;
create index stockdaily_idx2 on stockdata.stockdaily(tradedate) tablespace indexspace;

commit;