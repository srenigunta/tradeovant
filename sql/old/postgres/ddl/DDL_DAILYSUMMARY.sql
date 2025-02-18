create materialized view rawdata.dailytickersummary (
sector,companyname,industry, symbol,atr,avgvolume,beta,booksh,cashsh,"change",currentratio,debteq,dividend,dividendperc,earnings,employees,epsnext5y,epsnextq,epspast5y,epsqq,epsthisy,epsttm,forwardpe,ftwhigh,ftwlow,ftwrange,grossmargin,income,"index",insiderown,insidertrans,instown,insttrans,ltdebteq,marketcap,opermargin,optionable,payout,pb,pc,pe,peg,perfhalf,perfmonth,perfquarter,perfweek,perfyear,perfytd,pfcf,prevclose,price,profitmargin,ps,quickratio,recom,relvolume,roa,roe,roi,rsi,sales,salespast5y,salesqq,shortable,shortfloat,shortratio,shsfloat,shsoutstand,sma20,sma200,sma50,targetprice,volatility,volume
)
tablespace dataspaces
as
(
select cpy.sector, cpy.company, cpy.industry, ct.*
from (
select * 
from crosstab
(
	'select f.symbol::text, ra.attributealias::text, replace(case when length(f.datavalue) = 1 and f.datavalue = ''-'' then null else f.datavalue end, ''%'', '''')::text
	from refdata.ref_attributes ra 
	inner join rawdata.finvizsummary f 
	on f.columnname = ra.attributename
	where ra.attributealias <> ''epsnexty''
	order by 1, 2'
)
as ct("symbol" text, "atr" text, 	"avgvolume" text, 	"beta" text, 	"booksh" text, 	"cashsh" text, 	"change" text, 	"currentratio" text, 	"debteq" text, 	"dividend" text, 	"dividendperc" text, 	"earnings" text, 	"employees" text, 	"epsnext5y" text, 	"epsnextq" text, 	"epspast5y" text, 	"epsqq" text, 	"epsthisy" text, 	"epsttm" text, 	"forwardpe" text, 	"ftwhigh" text, 	"ftwlow" text, 	"ftwrange" text, 	"grossmargin" text, 	"income" text, 	"index" text, 	"insiderown" text, 	"insidertrans" text, 	"instown" text, 	"insttrans" text, 	"ltdebteq" text, 	"marketcap" text, 	"opermargin" text, 	"optionable" text, 	"payout" text, 	"pb" text, 	"pc" text, 	"pe" text, 	"peg" text, 	"perfhalf" text, 	"perfmonth" text, 	"perfquarter" text, 	"perfweek" text, 	"perfyear" text, 	"perfytd" text, 	"pfcf" text, 	"prevclose" text, 	"price" text, 	"profitmargin" text, 	"ps" text, 	"quickratio" text, 	"recom" text, 	"relvolume" text, 	"roa" text, 	"roe" text, 	"roi" text, 	"rsi" text, 	"sales" text, 	"salespast5y" text, 	"salesqq" text, 	"shortable" text, 	"shortfloat" text, 	"shortratio" text, 	"shsfloat" text, 	"shsoutstand" text, 	"sma20" text, 	"sma200" text, 	"sma50" text, 	"targetprice" text, "volatility" text, 	"volume" text 
)
) ct 
inner join rawdata.finvizsignals_basic cpy 
on cast(cpy.ticker as text) = ct.symbol
);


create index dailytickersummary_idx1 on rawdata.dailytickersummary(symbol) tablespace indexspace;
create index dailytickersummary_idx2 on rawdata.dailytickersummary(sector) tablespace indexspace;

