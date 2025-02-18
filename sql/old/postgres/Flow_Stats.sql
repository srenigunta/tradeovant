create materialized view flow.opts_flow_stats(sector,tick_symb,tick_name,tick_indstry,etf_flg,earnings_date,tick_volume,float_shares,rel_volume,outstndg_shares,price_last,price_chng,
price_chng_perc,price_chng_wkly,price_chng_mthly,price_chng_ytd,rsi_14,atr_14,beta_1year,high_52week,low_52week,volatility_week,volatility_month,sma50,
sma200,shortfloat,shortratio,insiderown,flow_imp_dt,flow_day,flow_day_nbr,exp_wk_nbr,exp_wk_rownbr,opts_exp,opts_strk,opts_typ,opts_prem_imp_dt,opts_vol_imp_dt,opts_prem_side_imp_dt,opts_vol_side_imp_dt,opts_prem_mny_imp_dt,opts_vol_mny_imp_dt,
call_vol_i_perc,dtls_side,bull_side,opts_iv_imp_dt,unusual_flg,opts_oi,prev_oi,nextday_oi,last_oi,delta_chng_vol,bull_side_i_vol,bull_side_i_prem,ask_vol_tsei,bid_vol_tsei,mid_vol_tsei,ask_vol_tsei_perc,bid_vol_tsei_perc,mid_vol_tsei_perc,ask_prem_tsei,bid_prem_tsei,mid_prem_tsei,ask_prem_tsei_perc,bid_prem_tsei_perc,mid_prem_tsei_perc,ask_vol_tse,bid_vol_tse,mid_vol_tse,ask_vol_tse_perc,bid_vol_tse_perc,mid_vol_tse_perc,ask_prem_tse,bid_prem_tse,mid_prem_tse,ask_prem_tse_perc,bid_prem_tse_perc,mid_prem_tse_perc)
tablespace dataspaces
as
(
select distinct 
	case when flow.etf_flg = 'True' then 'ETFs' else ds.sector end as sector,
	ds.symbol as TICK_SYMB,
	ds.companyname AS TICK_NAME,
	ds.industry,
	flow.etf_flg,
	to_char(to_date(replace(replace(ds.earnings, ' BMO', ''), ' AMC', '')||' 2021', 'Mon dd yyyy'), 'mm/dd/yyyy') as EARNINGS_DATE,
	ds.volume as TICK_VOLUME,
	ds.shsfloat as FLOAT_SHARES,
	ds.relvolume as REL_VOLUME,
	ds.shsoutstand as OUTSTNDG_SHARES,
	ds.price as PRICE_LAST,
	case when ds.price <> '-' then cast (ds.price as decimal(10, 2)) - cast (ds.prevclose as decimal(10, 2)) else 0 end as PRICE_CHNG,
	cast(ds."change" as decimal(10, 4))/100 as PRICE_CHNG_PERC,
	cast(ds.perfweek as decimal(10, 4))/100 as PRICE_CHNG_WKLY,
	cast(ds.perfmonth as decimal(10, 4))/100 as PRICE_CHNG_MTHLY,
	cast(ds.perfytd as decimal(10, 4))/100 as PRICE_CHNG_YTD,
	ds.rsi as RSI_14,
	ds.atr as ATR_14,
	ds.beta as BETA_1YEAR,
	cast(ds.ftwhigh as decimal(10, 4))/100 as HIGH_52WEEK,
	cast(ds.ftwlow as decimal(10, 4))/100 as LOW_52WEEK,
	cast(replace(case when position(' ' in ds.volatility) <> 0 then substring(ds.volatility, 1, position(' ' in ds.volatility)-1) end, '-', '0') as decimal(10, 4))/100 as VOLATILITY_WEEK,
	cast(replace(case when position(' ' in ds.volatility) <> 0 then substring(ds.volatility, position(' ' in ds.volatility)) end, '-', '0') as decimal(10, 4))/100 as VOLATILITY_MONTH,
	cast(ds.sma50 as decimal(10, 4))/100 as sma50,
	cast(ds.sma200 as decimal(10, 4))/100 as sma200,
	cast(ds.shortfloat as decimal(10, 4))/100 as shortfloat,
	ds.shortratio,
	cast(ds.insiderown as decimal(10, 4))/100 as insiderown,
	flow.FLOW_IMP_DT,
    TO_CHAR(TO_DATE(flow.FLOW_IMP_DT, 'MM/DD/YYYY'), 'DY') AS FLOW_DAY, 
    TO_CHAR(TO_DATE(flow.FLOW_IMP_DT, 'MM/DD/YYYY'), 'D') AS FLOW_DAY_NBR, 
    ((select d.last_day_of_week from refdata.datedim d where d.actualdate = to_date(flow.opts_exp, 'mm/dd/yyyy')) - (select d.last_day_of_week from refdata.datedim d where d.actualdate = current_date + 1)) /7 +1 as EXP_WK_NBR,
    dense_rank() over (partition by ds.symbol order by ((select last_day_of_week from refdata.datedim d where actualdate = to_date(opts_exp, 'mm/dd/yyyy')) - (select last_day_of_week from refdata.datedim d where actualdate = current_date + 1)) /7 +1) as EXP_WK_ROWNBR,
	flow.OPTS_EXP, 
    flow.OPTS_STRK, 
    flow.OPTS_TYP, 
    flow.OPTS_PREM_IMP_DT, 
    flow.OPTS_VOL_IMP_DT, 
    flow.OPTS_PREM_SIDE_IMP_DT, 
    flow.OPTS_VOL_SIDE_IMP_DT, 
    flow.OPTS_PREM_MNY_IMP_DT,
    flow.OPTS_VOL_MNY_IMP_DT,
    flow.CALL_VOL_I_PERC, 
    case when flow.DTLS_SIDE = 'A' then 'Ask' when flow.DTLS_SIDE = 'B' then 'Bid' else 'Mid' end AS DTLS_SIDE,
    flow.BULL_SIDE, 
    flow.OPTS_IV_IMP_DT, 
    flow.UNUSUAL_FLG, 
    flow.OPTS_OI, 
    flow.PREV_OI, 
    flow.NEXTDAY_OI, 
    flow.LAST_OI, 
    flow.DELTA_CHNG_VOL,
    CASE WHEN flow.BULL_SIDE = 'Bullish' THEN flow.OPTS_VOL_SIDE_IMP_DT ELSE 0 END AS BULL_SIDE_I_VOL,
    CASE WHEN flow.BULL_SIDE = 'Bullish' THEN flow.OPTS_PREM_SIDE_IMP_DT ELSE 0 END AS BULL_SIDE_I_PREM,
    flow.ASK_VOL_TSEI, 
    flow.BID_VOL_TSEI, 
    flow.MID_VOL_TSEI, 
    flow.ASK_VOL_TSEI_PERC, 
    flow.BID_VOL_TSEI_PERC, 
    flow.MID_VOL_TSEI_PERC, 
    flow.ASK_PREM_TSEI, 
    flow.BID_PREM_TSEI, 
    flow.MID_PREM_TSEI, 
    flow.ASK_PREM_TSEI_PERC, 
    flow.BID_PREM_TSEI_PERC, 
    flow.MID_PREM_TSEI_PERC,
    flow.ASK_VOL_TSE, 
    flow.BID_VOL_TSE, 
    flow.MID_VOL_TSE, 
    flow.ASK_VOL_TSE_PERC, 
    flow.BID_VOL_TSE_PERC, 
    flow.MID_VOL_TSE_PERC, 
    flow.ASK_PREM_TSE, 
    flow.BID_PREM_TSE, 
    flow.MID_PREM_TSE, 
    flow.ASK_PREM_TSE_PERC, 
    flow.BID_PREM_TSE_PERC, 
    flow.MID_PREM_TSE_PERC,
    case when to_char(max(cast(flow.flow_imp_dt as date)) over (partition by null), 'MM/DD/YYYY') = flow.flow_imp_dt then 'Y' else 'N' end as ltstflowdt

from rawdata.dailytickersummary ds 
inner join
(
SELECT DISTINCT FL.FLOW_IMP_DT, FL.TICK_INDSTRY, FL.ER_DATE, FL.ETF_FLG, FL.TICK_SYMB, FL.OPTS_EXP, FL.OPTS_STRK, FL.OPTS_TYP, FL.OPTS_PREM_IMP_DT, FL.OPTS_VOL_IMP_DT, FL.OPTS_PREM_SIDE_IMP_DT, FL.OPTS_VOL_SIDE_IMP_DT,
    CASE
    WHEN TOT_VOL_I = CALL_VOL_I
    THEN 1
    WHEN CALL_VOL_I = 0
    THEN 0
    ELSE ROUND((TOT_VOL_I - PUT_VOL_I)/NULLIF(TOT_VOL_I, 0), 4) 
    END AS CALL_VOL_I_PERC, 
    FL.DTLS_SIDE, 
    FL.BULL_SIDE,
    FL.OPTS_IV_IMP_DT, 
    MAX(CASE WHEN FL.OPTS_VOL_IMP_DT > FL.OPTS_OI THEN 'Yes' ELSE 'No' END) OVER (PARTITION BY FL.TICK_SYMB, FL.OPTS_EXP, FL.OPTS_STRK, FL.OPTS_TYP, FL.FLOW_IMP_DT) AS UNUSUAL_FLG,
    FL.OPTS_OI,
    OI.PREV_OI,
    OI.NEXTDAY_OI,
    OI.LAST_OI,
    CASE
            WHEN FL.OPTS_OI IS NOT NULL AND OI.NEXTDAY_OI IS NOT NULL AND OI.NEXTDAY_OI < (FL.OPTS_VOL_IMP_DT + FL.OPTS_OI) AND ROW_NUMBER() OVER (PARTITION BY FL.FLOW_IMP_DT, FL.TICK_SYMB, FL.OPTS_EXP, FL.OPTS_STRK, FL.OPTS_TYP ORDER BY OPTS_VOL_SIDE_IMP_DT DESC) = 1
            THEN (FL.OPTS_VOL_IMP_DT + FL.OPTS_OI) - NEXTDAY_OI
            ELSE 0
        END AS DELTA_CHNG_VOL,
    ROUND(ASK_VOL_TSEI, 0) AS ASK_VOL_TSEI,
    ROUND(BID_VOL_TSEI, 0) AS BID_VOL_TSEI, 
    ROUND(MID_VOL_TSEI, 0) AS MID_VOL_TSEI, 
    ROUND(ASK_VOL_TSEI_PERC, 4) AS ASK_VOL_TSEI_PERC, 
    ROUND(BID_VOL_TSEI_PERC, 4) AS BID_VOL_TSEI_PERC, 
    ROUND(MID_VOL_TSEI_PERC, 4) AS MID_VOL_TSEI_PERC, 
    ROUND(ASK_PREM_TSEI, 0) AS ASK_PREM_TSEI, 
    ROUND(BID_PREM_TSEI, 0) AS BID_PREM_TSEI, 
    ROUND(MID_PREM_TSEI, 0) AS MID_PREM_TSEI, 
    ROUND(ASK_PREM_TSEI_PERC, 4) AS ASK_PREM_TSEI_PERC, 
    ROUND(BID_PREM_TSEI_PERC, 4) AS BID_PREM_TSEI_PERC, 
    ROUND(MID_PREM_TSEI_PERC, 4) AS MID_PREM_TSEI_PERC,
    ROUND(ASK_VOL_TSE, 0) AS ASK_VOL_TSE, 
    ROUND(BID_VOL_TSE, 0) AS BID_VOL_TSE, 
    ROUND(MID_VOL_TSE, 0) AS MID_VOL_TSE, 
    ROUND(ASK_VOL_TSE_PERC, 4) AS ASK_VOL_TSE_PERC, 
    ROUND(BID_VOL_TSE_PERC, 4) AS BID_VOL_TSE_PERC, 
    ROUND(MID_VOL_TSE_PERC, 4) AS MID_VOL_TSE_PERC, 
    ROUND(ASK_PREM_TSE, 0) AS ASK_PREM_TSE, 
    ROUND(BID_PREM_TSE, 0) AS BID_PREM_TSE, 
    ROUND(MID_PREM_TSE, 0) AS MID_PREM_TSE, 
    ROUND(ASK_PREM_TSE_PERC, 4) AS ASK_PREM_TSE_PERC, 
    ROUND(BID_PREM_TSE_PERC, 4) AS BID_PREM_TSE_PERC, 
    ROUND(MID_PREM_TSE_PERC, 4) AS MID_PREM_TSE_PERC,
    OPTS_PREM_MNY_IMP_DT,
    OPTS_VOL_MNY_IMP_DT
from
(
	select distinct 
	FL.FLOW_IMP_DT, FL.ER_DATE, FL.ETF_FLG, FL.TICK_SYMB, FL.OPTS_EXP, FL.OPTS_STRK, FL.OPTS_TYP, FL.DTLS_SIDE, FL.BULL_SIDE, FL.TICK_INDSTRY, OPTS_OI,
	SUM(fl.OPTS_PRICE*fl.OPTS_VOL*100) OVER (PARTITION BY fl.FLOW_IMP_DT, fl.TICK_SYMB, fl.OPTS_EXP, fl.OPTS_STRK, fl.OPTS_TYP) AS OPTS_PREM_IMP_DT,
	SUM(fl.OPTS_VOL) OVER (PARTITION BY fl.FLOW_IMP_DT, fl.TICK_SYMB, fl.OPTS_EXP, fl.OPTS_STRK, fl.OPTS_TYP) AS OPTS_VOL_IMP_DT,
	SUM(fl.OPTS_PRICE*fl.OPTS_VOL*100) OVER (PARTITION BY fl.FLOW_IMP_DT, fl.TICK_SYMB, fl.OPTS_EXP, fl.OPTS_STRK, fl.OPTS_TYP, dtls_side) AS OPTS_PREM_SIDE_IMP_DT,
	SUM(fl.OPTS_VOL) OVER (PARTITION BY fl.FLOW_IMP_DT, fl.TICK_SYMB, fl.OPTS_EXP, fl.OPTS_STRK, fl.OPTS_TYP, dtls_side) AS OPTS_VOL_SIDE_IMP_DT,
	SUM(CASE WHEN fl.OPTS_TYP = 'CALL' THEN fl.OPTS_VOL END) OVER (PARTITION BY fl.TICK_SYMB, fl.FLOW_IMP_DT) AS CALL_VOL_I,
	SUM(CASE WHEN fl.OPTS_TYP = 'PUT' THEN fl.OPTS_VOL END) OVER (PARTITION BY fl.TICK_SYMB, fl.FLOW_IMP_DT) AS PUT_VOL_I,
	SUM(fl.OPTS_VOL) OVER (PARTITION BY fl.TICK_SYMB, fl.FLOW_IMP_DT) AS TOT_VOL_I,
	SUM(fl.OPTS_PRICE*fl.OPTS_VOL*100) OVER (PARTITION BY fl.FLOW_IMP_DT, fl.TICK_SYMB, fl.OPTS_EXP, fl.OPTS_STRK, fl.OPTS_TYP, dtls_side, moneyness) AS OPTS_PREM_MNY_IMP_DT,
	SUM(fl.OPTS_VOL) OVER (PARTITION BY fl.FLOW_IMP_DT, fl.TICK_SYMB, fl.OPTS_EXP, fl.OPTS_STRK, fl.OPTS_TYP, dtls_side, moneyness) AS OPTS_VOL_MNY_IMP_DT,
	coalesce(SUM(CASE WHEN fl.dtls_side LIKE '%A%' THEN fl.opts_vol END) OVER (PARTITION BY fl.tick_symb, fl.opts_typ, fl.flow_imp_dt, fl.opts_exp, fl.opts_strk), 0) AS ASK_VOL_TSEI,
	coalesce(SUM(CASE WHEN fl.dtls_side LIKE '%B%' THEN fl.opts_vol END) OVER (PARTITION BY fl.tick_symb, fl.opts_typ, fl.flow_imp_dt, fl.opts_exp, fl.opts_strk), 0) AS BID_VOL_TSEI,
	coalesce(SUM(CASE WHEN fl.dtls_side LIKE '%M%' THEN fl.opts_vol END) OVER (PARTITION BY fl.tick_symb, fl.opts_typ, fl.flow_imp_dt, fl.opts_exp, fl.opts_strk), 0) AS MID_VOL_TSEI,
	coalesce(ROUND(SUM(CASE WHEN fl.dtls_side LIKE '%A%' THEN fl.opts_vol END) OVER (PARTITION BY fl.tick_symb, fl.opts_typ, fl.flow_imp_dt, fl.opts_exp, fl.opts_strk)/NULLIF(SUM(fl.opts_vol) OVER (PARTITION BY fl.tick_symb, fl.opts_typ, fl.flow_imp_dt, fl.opts_exp, fl.opts_strk), 0), 4), 0) AS ASK_VOL_TSEI_PERC,
	coalesce(ROUND(SUM(CASE WHEN fl.dtls_side LIKE '%B%' THEN fl.opts_vol END) OVER (PARTITION BY fl.tick_symb, fl.opts_typ, fl.flow_imp_dt, fl.opts_exp, fl.opts_strk)/NULLIF(SUM(fl.opts_vol) OVER (PARTITION BY fl.tick_symb, fl.opts_typ, fl.flow_imp_dt, fl.opts_exp, fl.opts_strk), 0), 4), 0) AS BID_VOL_TSEI_PERC,
	coalesce(ROUND(SUM(CASE WHEN fl.dtls_side LIKE '%M%' THEN fl.opts_vol END) OVER (PARTITION BY fl.tick_symb, fl.opts_typ, fl.flow_imp_dt, fl.opts_exp, fl.opts_strk)/NULLIF(SUM(fl.opts_vol) OVER (PARTITION BY fl.tick_symb, fl.opts_typ, fl.flow_imp_dt, fl.opts_exp, fl.opts_strk), 0), 4), 0) AS MID_VOL_TSEI_PERC,
	coalesce(SUM(CASE WHEN fl.dtls_side LIKE '%A%' THEN fl.opts_price*fl.opts_vol*100 END) OVER (PARTITION BY fl.tick_symb, fl.opts_typ, fl.flow_imp_dt, fl.opts_exp, fl.opts_strk), 0) AS ASK_PREM_TSEI,
	coalesce(SUM(CASE WHEN fl.dtls_side LIKE '%B%' THEN fl.opts_price*fl.opts_vol*100 END) OVER (PARTITION BY fl.tick_symb, fl.opts_typ, fl.flow_imp_dt, fl.opts_exp, fl.opts_strk), 0) AS BID_PREM_TSEI,
	coalesce(SUM(CASE WHEN fl.dtls_side LIKE '%M%' THEN fl.opts_price*fl.opts_vol*100 END) OVER (PARTITION BY fl.tick_symb, fl.opts_typ, fl.flow_imp_dt, fl.opts_exp, fl.opts_strk), 0) AS MID_PREM_TSEI,
	coalesce(ROUND(SUM(CASE WHEN fl.dtls_side LIKE '%A%' THEN fl.opts_price*fl.opts_vol*100 END) OVER (PARTITION BY fl.tick_symb, fl.opts_typ, fl.flow_imp_dt, fl.opts_exp, fl.opts_strk)/NULLIF(SUM(fl.opts_price*fl.opts_vol*100) OVER (PARTITION BY fl.tick_symb, fl.opts_typ, fl.flow_imp_dt, fl.opts_exp, fl.opts_strk), 0), 4), 0) AS ASK_PREM_TSEI_PERC,
	coalesce(ROUND(SUM(CASE WHEN fl.dtls_side LIKE '%B%' THEN fl.opts_price*fl.opts_vol*100 END) OVER (PARTITION BY fl.tick_symb, fl.opts_typ, fl.flow_imp_dt, fl.opts_exp, fl.opts_strk)/NULLIF(SUM(fl.opts_price*fl.opts_vol*100) OVER (PARTITION BY fl.tick_symb, fl.opts_typ, fl.flow_imp_dt, fl.opts_exp, fl.opts_strk), 0), 4), 0) AS BID_PREM_TSEI_PERC,
	coalesce(ROUND(SUM(CASE WHEN fl.dtls_side LIKE '%M%' THEN fl.opts_price*fl.opts_vol*100 END) OVER (PARTITION BY fl.tick_symb, fl.opts_typ, fl.flow_imp_dt, fl.opts_exp, fl.opts_strk)/NULLIF(SUM(fl.opts_price*fl.opts_vol*100) OVER (PARTITION BY fl.tick_symb, fl.opts_typ, fl.flow_imp_dt, fl.opts_exp, fl.opts_strk), 0), 4), 0) AS MID_PREM_TSEI_PERC,
	ROUND(AVG(cast(fl.OPTS_IV as decimal(10, 2))) OVER (PARTITION BY fl.FLOW_IMP_DT, fl.TICK_SYMB, fl.OPTS_EXP, fl.OPTS_STRK, fl.OPTS_TYP, fl.dtls_side), 4) AS OPTS_IV_IMP_DT, 
	coalesce(SUM(CASE WHEN fl.dtls_side LIKE '%A%' THEN fl.opts_vol END) OVER (PARTITION BY fl.tick_symb, fl.opts_typ, fl.opts_exp, fl.opts_strk), 0) AS ASK_VOL_TSE,
	coalesce(SUM(CASE WHEN fl.dtls_side LIKE '%B%' THEN fl.opts_vol END) OVER (PARTITION BY fl.tick_symb, fl.opts_typ, fl.opts_exp, fl.opts_strk), 0) AS BID_VOL_TSE,
	coalesce(SUM(CASE WHEN fl.dtls_side LIKE '%M%' THEN fl.opts_vol END) OVER (PARTITION BY fl.tick_symb, fl.opts_typ, fl.opts_exp, fl.opts_strk), 0) AS MID_VOL_TSE,
	coalesce(ROUND(SUM(CASE WHEN fl.dtls_side LIKE '%A%' THEN fl.opts_vol END) OVER (PARTITION BY fl.tick_symb, fl.opts_typ, fl.opts_exp, fl.opts_strk)/NULLIF(SUM(fl.opts_vol) OVER (PARTITION BY fl.tick_symb, fl.opts_typ, fl.opts_exp, fl.opts_strk), 0), 4), 0) AS ASK_VOL_TSE_PERC,
	coalesce(ROUND(SUM(CASE WHEN fl.dtls_side LIKE '%B%' THEN fl.opts_vol END) OVER (PARTITION BY fl.tick_symb, fl.opts_typ, fl.opts_exp, fl.opts_strk)/NULLIF(SUM(fl.opts_vol) OVER (PARTITION BY fl.tick_symb, fl.opts_typ, fl.opts_exp, fl.opts_strk), 0), 4), 0) AS BID_VOL_TSE_PERC,
	coalesce(ROUND(SUM(CASE WHEN fl.dtls_side LIKE '%M%' THEN fl.opts_vol END) OVER (PARTITION BY fl.tick_symb, fl.opts_typ, fl.opts_exp, fl.opts_strk)/NULLIF(SUM(fl.opts_vol) OVER (PARTITION BY fl.tick_symb, fl.opts_typ, fl.opts_exp, fl.opts_strk), 0), 4), 0) AS MID_VOL_TSE_PERC,
	coalesce(SUM(CASE WHEN fl.dtls_side LIKE '%A%' THEN fl.opts_price*fl.opts_vol*100 END) OVER (PARTITION BY fl.tick_symb, fl.opts_typ, fl.opts_exp, fl.opts_strk), 0) AS ASK_PREM_TSE,
	coalesce(SUM(CASE WHEN fl.dtls_side LIKE '%B%' THEN fl.opts_price*fl.opts_vol*100 END) OVER (PARTITION BY fl.tick_symb, fl.opts_typ, fl.opts_exp, fl.opts_strk), 0) AS BID_PREM_TSE,
	coalesce(SUM(CASE WHEN fl.dtls_side LIKE '%M%' THEN fl.opts_price*fl.opts_vol*100 END) OVER (PARTITION BY fl.tick_symb, fl.opts_typ, fl.opts_exp, fl.opts_strk), 0) AS MID_PREM_TSE,
	coalesce(ROUND(SUM(CASE WHEN fl.dtls_side LIKE '%A%' THEN fl.opts_price*fl.opts_vol*100 END) OVER (PARTITION BY fl.tick_symb, fl.opts_typ, fl.opts_exp, fl.opts_strk)/NULLIF(SUM(fl.opts_price*fl.opts_vol*100) OVER (PARTITION BY fl.tick_symb, fl.opts_typ, fl.opts_exp, fl.opts_strk), 0), 4), 0) AS ASK_PREM_TSE_PERC,
	coalesce(ROUND(SUM(CASE WHEN fl.dtls_side LIKE '%B%' THEN fl.opts_price*fl.opts_vol*100 END) OVER (PARTITION BY fl.tick_symb, fl.opts_typ, fl.opts_exp, fl.opts_strk)/NULLIF(SUM(fl.opts_price*fl.opts_vol*100) OVER (PARTITION BY fl.tick_symb, fl.opts_typ, fl.opts_exp, fl.opts_strk), 0), 4), 0) AS BID_PREM_TSE_PERC,
	coalesce(ROUND(SUM(CASE WHEN fl.dtls_side LIKE '%M%' THEN fl.opts_price*fl.opts_vol*100 END) OVER (PARTITION BY fl.tick_symb, fl.opts_typ, fl.opts_exp, fl.opts_strk)/NULLIF(SUM(fl.opts_price*fl.opts_vol*100) OVER (PARTITION BY fl.tick_symb, fl.opts_typ, fl.opts_exp, fl.opts_strk), 0), 4), 0) AS MID_PREM_TSE_PERC
	from 
	(
		select distinct 
		tick_symb,
		exec_tm,
		opts_exp,
		cast(opts_strk as decimal(10, 2)) as opts_strk, 
		opts_typ,
		er_date,
		etf_flg,
		OPTS_OI,
		opts_iv,
		flow_imp_dt,
		TICK_INDSTRY,
		cast(opts_price as decimal(10, 2)) as opts_price,
		cast(opts_vol as decimal(10, 2)) as opts_vol, 
		trans_typ,
		case 
			when DTLS_SIDE = '' then 'M'
			when lower(DTLS_SIDE) = 'nan' then 'M'
			when REGEXP_REPLACE(DTLS_SIDE, '_', '') in ('A', 'AA') then 'A'
			when REGEXP_REPLACE(DTLS_SIDE, '_', '') in ('B', 'BB') then 'B'
			else REGEXP_REPLACE(DTLS_SIDE, '_', '')
		end as DTLS_SIDE,
		cast(tick_spot as decimal(10, 2)) as tick_spot,
		case 
			when opts_typ = 'CALL' and REGEXP_REPLACE(DTLS_SIDE, '_', '') like '%A%' then 'Bullish'
			when opts_typ = 'PUT' and REGEXP_REPLACE(DTLS_SIDE, '_', '') like '%B%' then 'Bullish'
			when opts_typ = 'CALL' and REGEXP_REPLACE(DTLS_SIDE, '_', '') like '%B%' then 'Bearish'
			when opts_typ = 'PUT' and REGEXP_REPLACE(DTLS_SIDE, '_', '') like '%A%' then 'Bearish'
			else 'Others'
		end as bull_side,
		case 
			when cast(tick_spot as decimal(10, 2)) < cast(opts_strk as decimal(10, 2)) and opts_typ = 'CALL'
			then 'OTM'
			when cast(tick_spot as decimal(10, 2)) >= cast(opts_strk as decimal(10, 2)) and opts_typ = 'CALL'
			then 'ITM'
			when cast(tick_spot as decimal(10, 2)) <= cast(opts_strk as decimal(10, 2)) and opts_typ = 'PUT'
			then 'ITM'
			when cast(tick_spot as decimal(10, 2)) > cast(opts_strk as decimal(10, 2)) and opts_typ = 'PUT'
			then 'OTM'
		end as moneyness
		from flow.ops_flow_imp ofi 
		where current_date - to_date(flow_imp_dt, 'mm/dd/yyyy') between 0 and 40
		and to_date(opts_exp, 'mm/dd/yyyy') >= current_date
		AND CASE 
				WHEN TO_DATE(OPTS_EXP, 'MM/DD/YYYY') = current_date
				AND cast(TO_CHAR(current_date, 'HH24') as integer) > 16 
				THEN 0 
				ELSE 1 
			END = 1
		and trans_typ NOT LIKE '%ML%'
	) fl
) fl
left outer join 
(
	SELECT DISTINCT TICK_SYMB, OPTS_OI, OPTS_EXP, OPTS_STRK, OPTS_TYP, FLOW_IMP_DT, PREV_OI, (OPTS_OI - PREV_OI) AS DELTA_OI, ROUND(((OPTS_OI - PREV_OI)/ NULLIF(OPTS_OI, 0))*100, 2) AS DELTA_OI_PERC, NEXTDAY_OI, LAST_OI, TOT_VOL
	from (
	select distinct 
	TICK_SYMB, OPTS_OI, OPTS_EXP, OPTS_STRK, OPTS_TYP, FLOW_IMP_DT, 
	    LAG(OPTS_OI) OVER (PARTITION BY TICK_SYMB, OPTS_EXP, OPTS_STRK, OPTS_TYP ORDER BY FLOW_IMP_DT) AS PREV_OI,
	    LEAD(OPTS_OI) OVER (PARTITION BY TICK_SYMB, OPTS_EXP, OPTS_STRK, OPTS_TYP ORDER BY FLOW_IMP_DT) AS NEXTDAY_OI,
	    MAX(LAST_OI) OVER (PARTITION BY TICK_SYMB, OPTS_EXP, OPTS_STRK, OPTS_TYP) AS LAST_OI, TOT_VOL
	from(
	select distinct 
	TICK_SYMB, OPTS_OI, OPTS_EXP, cast(OPTS_STRK as decimal(10, 2)) as OPTS_STRK, OPTS_TYP, FLOW_IMP_DT,
	MAX(TO_DATE(FLOW_IMP_DT, 'MM/DD/YYYY')) OVER (PARTITION BY NULL) AS MAX_DT,
	CASE 
		WHEN TO_DATE(FLOW_IMP_DT, 'MM/DD/YYYY') = MAX(TO_DATE(FLOW_IMP_DT, 'MM/DD/YYYY')) OVER (PARTITION BY TICK_SYMB, OPTS_EXP, OPTS_STRK, OPTS_TYP) THEN OPTS_OI 
	END AS LAST_OI,
	SUM(OPTS_VOL) OVER (PARTITION BY TICK_SYMB, OPTS_TYP, FLOW_IMP_DT, OPTS_EXP, OPTS_STRK) AS TOT_VOL
	from flow.ops_flow_imp ofi 
	where current_date - to_date(flow_imp_dt, 'mm/dd/yyyy') between 0 and 40
	and to_date(opts_exp, 'mm/dd/yyyy') >= current_date
	AND CASE 
			WHEN TO_DATE(OPTS_EXP, 'MM/DD/YYYY') = current_date
			AND cast(TO_CHAR(current_date, 'HH24') as integer) > 16 
			THEN 0 
			ELSE 1 
		END = 1
	) a
	) a
) oi
on OI.TICK_SYMB = FL.TICK_SYMB
AND OI.OPTS_EXP = FL.OPTS_EXP
AND OI.OPTS_STRK = FL.OPTS_STRK
AND OI.OPTS_TYP = FL.OPTS_TYP
AND OI.FLOW_IMP_DT = FL.FLOW_IMP_DT
)flow
on flow.TICK_SYMB = ds.symbol 
);


create index opts_flow_stats_idx1 on flow.opts_flow_stats(sector)
tablespace indexspace;

create index opts_flow_stats_idx2 on flow.opts_flow_stats(tick_symb)
tablespace indexspace;

create index opts_flow_stats_idx3 on flow.opts_flow_stats(flow_imp_dt)
tablespace indexspace;

create index opts_flow_stats_idx4 on flow.opts_flow_stats(opts_exp)
tablespace indexspace;

create index opts_flow_stats_idx5 on flow.opts_flow_stats(opts_typ)
tablespace indexspace;

create index opts_flow_stats_idx6 on flow.opts_flow_stats(earnings_date)
tablespace indexspace;


grant select on flow.opts_flow_stats to biuser;

insert into rawdata.dailytickersummary (
sector,companyname,industry, symbol,atr,avgvolume,beta,booksh,cashsh,"change",currentratio,debteq,dividend,dividendperc,earnings,employees,epsnext5y,epsnextq,epspast5y,epsqq,epsthisy,epsttm,forwardpe,ftwhigh,ftwlow,ftwrange,grossmargin,income,"index",insiderown,insidertrans,instown,insttrans,ltdebteq,marketcap,opermargin,optionable,payout,pb,pc,pe,peg,perfhalf,perfmonth,perfquarter,perfweek,perfyear,perfytd,pfcf,prevclose,price,profitmargin,ps,quickratio,recom,relvolume,roa,roe,roi,rsi,sales,salespast5y,salesqq,shortable,shortfloat,shortratio,shsfloat,shsoutstand,sma20,sma200,sma50,targetprice,volatility,volume
)

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

