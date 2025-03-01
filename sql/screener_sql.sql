with optionscreener as 
(
	select 
		ticker,
		case when put_call_ratio = 0 then round(put_volume/nullif(call_volume, 0), 4) else put_call_ratio end as put_call_ratio,
		call_volume,
		call_volume_ask_side,
		call_volume_bid_side,
		round(call_volume_ask_side/NULLIF(call_volume, 0), 4) as call_ask_perc,
		avg_3_day_call_volume,
		round(call_volume/NULLIF(avg_3_day_call_volume, 0), 4) as call_volume_spike_3d,
		avg_7_day_call_volume,
		round(call_volume/NULLIF(avg_7_day_call_volume, 0), 4) as call_volume_spike_7d,
		avg_30_day_call_volume,
		round(call_volume/NULLIF(avg_30_day_call_volume, 0), 4) as call_volume_spike_30d,
		put_volume,
		put_volume_ask_side,
		put_volume_bid_side,
		round(put_volume_ask_side/NULLIF(put_volume, 0), 4) as put_ask_perc,
		avg_3_day_put_volume,
		round(put_volume/NULLIF(avg_3_day_put_volume, 0), 4) as put_volume_spike_3d,
		avg_7_day_put_volume,
		round(put_volume/NULLIF(avg_7_day_put_volume, 0), 4) as put_volume_spike_7d,
		avg_30_day_put_volume,
		round(put_volume/NULLIF(avg_30_day_put_volume, 0), 4) as put_volume_spike_30d,
		(call_volume + put_volume) as options_tot_volume,
		(call_volume_ask_side + put_volume_bid_side) as bullish_volume,
		round((call_volume_ask_side + put_volume_bid_side)/NULLIF((call_volume + put_volume) , 0), 4) as bullish_volume_perc,
		(call_volume_bid_side + put_volume_ask_side) as bearish_volume,
		round((call_volume_bid_side + put_volume_ask_side)/NULLIF((call_volume + put_volume) , 0), 4) as bearish_volume_perc,
		call_open_interest,
		prev_call_oi,
		round((call_open_interest-prev_call_oi)/NULLIF(prev_call_oi, 0), 4) as call_oi_delta_perc,
		put_open_interest,
		prev_put_oi,
		round((put_open_interest-prev_put_oi)/NULLIF(prev_put_oi, 0), 4) as put_oi_delta_perc,
		round((call_volume_ask_side - call_volume_bid_side) / NULLIF(call_volume + put_volume, 0), 4) AS call_skew,
		round((put_volume_bid_side - put_volume_ask_side) / NULLIF(call_volume + put_volume, 0), 4) AS put_skew,
		total_open_interest,
		bullish_premium,
		bearish_premium,
		call_premium,
		net_call_premium,
		put_premium,
		net_put_premium,
		avg_30_day_put_oi,
		round(put_open_interest/NULLIF(avg_30_day_put_oi, 0), 4) as put_oi_spike_30d,
		avg_30_day_call_oi,
		round(call_open_interest/NULLIF(avg_30_day_call_oi, 0), 4) as call_oi_spike_30d,
		avg30_volume,
		volatility,
		iv30d,
		round((volatility - iv30d)/NULLIF(volatility, 0), 4) as iv_spike_d,
		iv30d_1m,
		round((volatility - iv30d_1m)/NULLIF(volatility, 0), 4) as iv_spike_30d,
		iv_rank,
		file_version_date
	from
	(
	    select 
	        ticker,
	        round(try_cast(put_call_ratio as double), 4) as put_call_ratio,
	        try_cast(call_volume as bigint) as call_volume,
	        try_cast(call_volume_ask_side as bigint) as call_volume_ask_side,
	        try_cast(call_volume_bid_side as bigint) as call_volume_bid_side,
	        try_cast(call_open_interest as bigint) as call_open_interest,
	        try_cast(prev_call_oi as bigint) as prev_call_oi,
	        try_cast(put_volume as bigint) as put_volume,
	        try_cast(put_volume_ask_side as bigint) as put_volume_ask_side,
	        try_cast(put_volume_bid_side as bigint) as put_volume_bid_side,
	        try_cast(put_open_interest as bigint) as put_open_interest,
	        try_cast(prev_put_oi as bigint) as prev_put_oi,
	        try_cast(total_open_interest as bigint) as total_open_interest,
	        try_cast(bullish_premium as bigint) as bullish_premium,
	        try_cast(bearish_premium as bigint) as bearish_premium,
	        try_cast(call_premium as bigint) as call_premium,
	        try_cast(net_call_premium as bigint) as net_call_premium,
	        try_cast(put_premium as bigint) as put_premium,
	        try_cast(net_put_premium as bigint) as net_put_premium,
	        round(try_cast(avg_3_day_call_volume as double), 4) as avg_3_day_call_volume,
	        round(try_cast(avg_3_day_put_volume as double), 4) as avg_3_day_put_volume,
	        round(try_cast(avg_7_day_call_volume as double), 4) as avg_7_day_call_volume,
	        round(try_cast(avg_7_day_put_volume as double), 4) as avg_7_day_put_volume,
	        round(try_cast(avg_30_day_call_volume as double), 4) as avg_30_day_call_volume,
	        round(try_cast(avg_30_day_put_volume as double), 4) as avg_30_day_put_volume,
	        round(try_cast(avg_30_day_put_oi as double), 4) as avg_30_day_put_oi,
	        round(try_cast(avg_30_day_call_oi as double), 4) as avg_30_day_call_oi,
	        round(try_cast(close as double), 4) as close,
	        round(try_cast(high as double), 4) as high,
	        round(try_cast(low as double), 4) as low,
	        round(try_cast(avg30_volume as double), 4) as avg30_volume,
	        round(try_cast(prev_close as double), 4) as prev_close,
	        round(try_cast(week_52_high as double), 4) as week_52_high,
	        round(try_cast(week_52_low as double), 4) as week_52_low,
	        round(try_cast(implied_move as double), 4) as implied_move,
	        round(try_cast(implied_move_perc as double), 4) as implied_move_perc,
	        round(try_cast(volatility as double), 4) as volatility,
	        round(try_cast(iv30d as double), 4) as iv30d,
	        round(try_cast(iv30d_1d as double), 4) as iv30d_1d,
	        round(try_cast(iv30d_1w as double), 4) as iv30d_1w,
	        round(try_cast(iv30d_1m as double), 4) as iv30d_1m,
	        round(try_cast(iv_rank as double), 4) as iv_rank,
	        sector,
	        full_name,
	        issue_type,
	        is_index,
	        try_cast(next_earnings_date as date) as next_earnings_date,
	        er_time,
	        file_version_date
	    FROM read_parquet("R:\local_bucket\raw_store\whales\stockscreener\parquet\*\*.parquet", hive_partitioning = True)
	    where 1 = 1
	    --and ticker = 'CELH'
	    and file_version_date = '20250224'
	) op
),
sweep_and_prem_data
as
(
    select distinct 
        symbol, 
        file_version_date,
        sum(case when option_tx_type = 'sweep' and option_type = 'call' then volume end) over (partition by symbol, file_version_date) as call_sweep_volume,
        sum(case when option_tx_type = 'sweep' and option_type = 'call' and side = 'ask' then volume end) over (partition by symbol, file_version_date) as call_ask_sweep_volume,
        sum(case when option_tx_type = 'sweep' and option_type = 'call' and side = 'bid' then volume end) over (partition by symbol, file_version_date) as call_bid_sweep_volume,
        sum(case when option_tx_type = 'sweep' and option_type = 'put' then volume end) over (partition by symbol, file_version_date) as put_sweep_volume,
        sum(case when option_tx_type = 'sweep' and option_type = 'put' and side = 'ask' then volume end) over (partition by symbol, file_version_date) as put_ask_sweep_volume,
        sum(case when option_tx_type = 'sweep' and option_type = 'put' and side = 'bid' then volume end) over (partition by symbol, file_version_date) as put_bid_sweep_volume,
        sum(case when option_tx_type = 'sweep' and option_type = 'call' then premium end) over (partition by symbol, file_version_date) as call_sweep_premium,
        sum(case when option_tx_type = 'sweep' and option_type = 'put' then premium end) over (partition by symbol, file_version_date) as put_sweep_premium,
        sum(case when option_tx_type = 'sweep' and option_type = 'call' and side = 'ask' then premium end) over (partition by symbol, file_version_date) as call_ask_sweep_premium,
        sum(case when option_tx_type = 'sweep' and option_type = 'put' and side = 'ask' then premium end) over (partition by symbol, file_version_date) as put_ask_sweep_premium,
        sum(case when option_tx_type = 'sweep' and option_type = 'call' and side = 'bid' then premium end) over (partition by symbol, file_version_date) as call_bid_sweep_premium,
        sum(case when option_tx_type = 'sweep' and option_type = 'put' and side = 'bid' then premium end) over (partition by symbol, file_version_date) as put_bid_sweep_premium,
        sum(case when option_type = 'call' and side = 'ask' then premium end) over (partition by symbol, file_version_date) as call_ask_premium,
        sum(case when option_type = 'call' and side = 'bid' then premium end) over (partition by symbol, file_version_date) as call_bid_premium,
        sum(case when option_type = 'put' and side = 'ask' then premium end) over (partition by symbol, file_version_date) as put_ask_premium,
        sum(case when option_type = 'put' and side = 'bid' then premium end) over (partition by symbol, file_version_date) as put_bid_premium,
    from
    (
        SELECT distinct
            symbol,
            side,
            option_tx_type,
            option_type,
            file_version_date,
            sum(volume) as volume,
            sum(premium) as premium
            from (
                    select 
                        underlying_symbol as symbol,
                        option_chain_id,
                        side,
                        coalesce(case 
                                    when upstream_condition_detail in ('mlet', 'mlat', 'mlct', 'tlct', 'tlft', 'tlet', 'tlat') then 'auto_multileg'
                                    when upstream_condition_detail in ('slan', 'slai', 'mesl', 'slcn', 'slft', 'mfsl') then 'sweep'
                                    when upstream_condition_detail in ('isoi', 'mlft') then 'manual_multileg'
                                    else 'standard'
                                end, 'all') as option_tx_type,
                        case 
                            when option_type = 'call' and side = 'ask' then 'bull'
                            when option_type = 'put' and side = 'bid' then 'bull'
                            when option_type = 'call' and side = 'bid' then 'bear'
                            when option_type = 'put' and side = 'ask' then 'bear'
                            else 'neutral'
                        end as op_side,
                        option_type,
                        try_cast(size as bigint) as volume,
                        try_cast(premium as double) as premium,
                        file_version_date
                    from read_parquet("R:\local_bucket\raw_store\whales\optionsflow\parquet\*\*.parquet", hive_partitioning = True) 
                    where 1 = 1
                    --and underlying_symbol = 'CELH'
                    and file_version_date = '20250224'
                )
        group by symbol, side, option_tx_type, option_type, file_version_date
    )
),
symbol_sector 
as
(
select distinct
    symbol,
    coalesce(name, max_name) as name,
    sector,
    industry
from
    (
        select distinct
            symbol,
            name,
            max(name) over (
                partition by
                    symbol
            ) as max_name,
            sector,
            industry,
            source_data,
            rank() over (
                partition by
                    symbol
                order by
                    priority,
                    sector,
                    industry,
                    name desc nulls last
            ) as row_ranking
        from
            (
                SELECT
                    symbol,
                    case
                        when length(co_name) <= 2 then null
                        else co_name
                    end as name,
                    case
                        when length(sector) <= 1
                        and length(industry) <= 1 then null
                        when length(sector) <= 1
                        and length(industry) > 1 then max(
                            case
                                when sector = 'Financial Services' then 'Financial'
                                else sector
                            end
                        ) over (
                            partition by
                                industry
                        )
                        when sector = 'Financial Services' then 'Financial'
                        else sector
                    end as sector,
                    case
                        when length(industry) <= 1 then null
                        when industry = 'Gambling, Resorts & Casinos' then 'Gambling'
                        else industry
                    end as industry,
                    'stockinvest' as source_data,
                    1 as priority
                FROM
                    read_parquet (
                        'R:\local_bucket\stage_store\stockinvest_us\screener_data\*\*.parquet',
                        hive_partitioning = True
                    )
                UNION
                SELECT
                    symbol,
                    case
                        when length(name) <= 2 then null
                        else name
                    end as name,
                    case
                        when length(sector) <= 1 then null
                        else sector
                    end as sector,
                    case
                        when length(industry) <= 1 then null
                        when industry = 'Apparel Manufacturing' then 'Apparel - Manufacturers'
                        when industry = 'Apparel Retail' then 'Apparel - Retail'
                        when industry = 'Auto & Truck Dealerships' then 'Auto - Dealerships'
                        when industry = 'Auto Manufacturers' then 'Auto - Manufacturers'
                        when industry = 'Auto Parts' then 'Auto - Parts'
                        when industry = 'Home Improvement Retail' then 'Home Improvement'
                        when industry = 'Oil & Gas E&P' then 'Oil & Gas Exploration & Production'
                        when industry = 'Paper & Paper Products' then 'Paper, Lumber & Forest Products'
                        when industry = 'Thermal Coal' then 'Coal'
                        else industry
                    end as industry,
                    'finviz' as source_data,
                    2 as priority
                FROM
                    read_parquet (
                        'R:\local_bucket\stage_store\finviz\fundamentals\*\*.parquet',
                        hive_partitioning = True
                    )
                UNION
                SELECT
                    symbol,
                    case
                        when length(name) <= 2 then null
                        else name
                    end as name,
                    case
                        when length(sector) <= 1 then null
                        when sector = 'Consumer Discretionary' then 'Consumer Cyclical'
                        when sector = 'Consumer Staples' then 'Consumer Defensive'
                        when sector = 'Materials' then 'Basic Materials'
                        when sector = 'Financial Services' then 'Financial'
                        when sector = 'Industrial' then 'Industrials'
                        when sector = 'Health Care' then 'Healthcare'
                        else sector
                    end as sector,
                    case
                        when length(industry) <= 1 then null
                        when industry = 'Aerospace' then 'Aerospace & Defense'
                        when industry = 'Apparel Retailers' then 'Apparel - Retail'
                        when industry = 'Auto Parts' then 'Auto - Parts'
                        when industry = 'Automobiles' then 'Auto - Manufacturers'
                        when industry = 'Exploration & Production' then 'Oil & Gas Exploration & Production'
                        when industry = 'Furnishings' then 'Furnishings, Fixtures & Appliances'
                        when industry = 'Home Improvement Retailers' then 'Home Improvement'
                        when industry = 'Integrated Oil & Gas' then 'Oil & Gas Integrated'
                        when industry = 'Oil Equipment & Services' then 'Oil & Gas Equipment & Services'
                        when industry = 'Paper' then 'Paper, Lumber & Forest Products'
                        when industry = 'Specialty Retailers' then 'Specialty Retail'
                        when industry = 'Waste & Disposal Services' then 'Waste Management'
                        else industry
                    end as industry,
                    'stockcharts' as source_data,
                    3 as priority
                FROM
                    read_parquet (
                        'R:\local_bucket\stage_store\stockcharts\sctr_reports\*\*.parquet',
                        hive_partitioning = True
                    )
                UNION
                SELECT
                    ticker as symbol,
                    case
                        when length(full_name) <= 2 then null
                        else ARRAY_TO_STRING (
                            ARRAY (
                                SELECT
                                    UPPER(SUBSTRING (unnest.word, 1, 1)) || LOWER(SUBSTRING (unnest.word, 2))
                                FROM
                                    UNNEST (string_to_array (lower(full_name), ' ')) AS unnest (word)
                            ),
                            ' '
                        )
                    end as name,
                    case
                        when length(sector) <= 1 then null
                        when sector = 'Financial Services' then 'Financial'
                        else sector
                    end as sector,
                    null as industry,
                    'whales' as source_data,
                    4 as priority
                FROM
                    read_parquet (
                        'R:\local_bucket\raw_store\whales\stockscreener\parquet\*\*.parquet',
                        hive_partitioning = True
                    )
            ) sc
        order by
            1,
            6,
            3,
            4 nulls last
    ) sc
where
    row_ranking = 1
order by
    1,
    2,
    3,
    4
)
select distinct 
	os.ticker,
	put_call_ratio,
	options_tot_volume, 
	-- CALL Option Volume Stats
	call_volume, call_volume_ask_side, call_volume_bid_side, call_ask_perc, 
	sp.call_sweep_volume, sp.call_ask_sweep_volume, sp.call_bid_sweep_volume,
	round(sp.call_ask_sweep_volume / NULLIF(sp.call_sweep_volume, 0), 4) AS call_sweep_ask_perc,
	round(sp.call_sweep_volume / NULLIF(call_volume, 0), 4) AS call_sweep_tot_perc,
	avg_3_day_call_volume, call_volume_spike_3d, avg_7_day_call_volume, call_volume_spike_7d, 
	avg_30_day_call_volume, call_volume_spike_30d, call_skew,
	-- PUT Option Volume Stats
	put_volume, put_volume_ask_side, put_volume_bid_side, put_ask_perc, 
	sp.put_sweep_volume, sp.put_ask_sweep_volume, sp.put_bid_sweep_volume,
	round(sp.put_ask_sweep_volume / NULLIF(sp.put_sweep_volume, 0), 4) AS put_sweep_ask_perc,
	round(sp.put_sweep_volume / NULLIF(put_volume, 0), 4) AS put_sweep_tot_perc,
	avg_3_day_put_volume, put_volume_spike_3d, avg_7_day_put_volume, put_volume_spike_7d, 
	avg_30_day_put_volume, put_volume_spike_30d, put_skew,
	-- Open Interest Stats
	total_open_interest, call_open_interest, prev_call_oi, call_oi_delta_perc, avg_30_day_call_oi, call_oi_spike_30d,
	put_open_interest,prev_put_oi, put_oi_delta_perc, avg_30_day_put_oi, put_oi_spike_30d,
	-- CALL Option Premium Stats
	call_premium, sp.call_ask_premium, sp.call_bid_premium, net_call_premium, 
	sp.call_sweep_premium, sp.call_ask_sweep_premium, sp.call_bid_sweep_premium,
	round(sp.call_sweep_premium/NULLIF(call_premium, 0), 4) as call_sweep_premium_perc,
	-- PUT Option Premium Stats
	put_premium, sp.put_ask_premium, sp.put_bid_premium, net_put_premium,
	sp.put_sweep_premium, sp.put_ask_sweep_premium, sp.put_bid_sweep_premium,
	round(sp.put_sweep_premium/NULLIF(put_premium, 0), 4) as put_sweep_premium_perc,
	-- Bullish Bearish Stats
	bullish_volume, bullish_volume_perc, bearish_volume, bearish_volume_perc, 
	(sp.call_ask_sweep_volume + sp.put_bid_sweep_volume) as bullish_sweep_volume,
	round((sp.call_ask_sweep_volume + sp.put_bid_sweep_volume)/NULLIF(bullish_volume , 0), 4) as bullish_sweep_volume_perc,
	round((sp.call_bid_sweep_volume + sp.put_ask_sweep_volume)/NULLIF(bearish_volume , 0), 4) as bullish_sweep_volume_perc,
	bullish_premium, bearish_premium, 
	-- Volatility Stats
	volatility, iv30d, iv_spike_d, iv30d_1m, iv_spike_30d, iv_rank,
    round(PERCENT_RANK() OVER (PARTITION BY ss.sector ORDER BY call_volume_spike_30d), 4) AS call_volume_spike_percentile,
    round(PERCENT_RANK() OVER (PARTITION BY ss.sector ORDER BY put_volume_spike_30d), 4)  AS put_volume_spike_percentile,
    round(PERCENT_RANK() OVER (PARTITION BY ss.sector ORDER BY call_ask_perc), 4)  AS call_ask_perc_percentile,
    round(PERCENT_RANK() OVER (PARTITION BY ss.sector ORDER BY put_ask_perc), 4) AS put_ask_perc_percentile,
    round(PERCENT_RANK() OVER (PARTITION BY ss.sector ORDER BY call_oi_delta_perc), 4)  AS call_oi_delta_percentile,
    round(PERCENT_RANK() OVER (PARTITION BY ss.sector ORDER BY put_oi_delta_perc), 4)  AS put_oi_delta_percentile,
    round(PERCENT_RANK() OVER (PARTITION BY ss.sector ORDER BY call_oi_spike_30d), 4)  AS call_oi_spike_percentile,
    round(PERCENT_RANK() OVER (PARTITION BY ss.sector ORDER BY put_oi_spike_30d), 4)  AS put_oi_spike_percentile,
    round(PERCENT_RANK() OVER (PARTITION BY ss.sector ORDER BY call_sweep_tot_perc), 4)  AS call_sweep_volume_ratio_percentile,
    round(PERCENT_RANK() OVER (PARTITION BY ss.sector ORDER BY put_sweep_tot_perc), 4)  AS put_sweep_volume_ratio_percentile,
    round(PERCENT_RANK() OVER (PARTITION BY ss.sector ORDER BY call_sweep_ask_perc), 4)  AS call_ask_sweep_ratio_percentile,
    round(PERCENT_RANK() OVER (PARTITION BY ss.sector ORDER BY put_sweep_ask_perc), 4)  AS put_ask_sweep_ratio_percentile,
    round(PERCENT_RANK() OVER (PARTITION BY ss.sector ORDER BY call_sweep_premium_perc), 4)  AS call_sweep_premium_ratio_percentile,
    round(PERCENT_RANK() OVER (PARTITION BY ss.sector ORDER BY put_sweep_premium_perc), 4)  AS put_sweep_premium_ratio_percentile,
	round((
      CASE WHEN call_volume_spike_30d > 2 THEN 0.5 ELSE 0 END +
      CASE WHEN call_ask_perc > 0.6 THEN 0.5 ELSE 0 END +
      CASE WHEN call_oi_delta_perc > 0.1 THEN 0.5 ELSE 0 END +
      CASE WHEN call_oi_spike_30d > 1.5 THEN 0.5 ELSE 0 END +
      CASE WHEN net_call_premium > 0 THEN 0.5 ELSE 0 END +
      CASE WHEN call_sweep_volume / NULLIF(call_volume, 0) > 0.3 THEN 0.5 ELSE 0 END +
      CASE WHEN round(sp.call_ask_sweep_volume / NULLIF(sp.call_sweep_volume, 0), 4) > 0.7 THEN 0.5 ELSE 0 END +
      CASE WHEN put_volume_spike_30d < 1 THEN 0.5 ELSE 0 END +
      CASE WHEN call_sweep_premium / NULLIF(call_premium, 0) > 0.5 THEN 0.5 ELSE 0 END +
      CASE WHEN call_sweep_volume > 2 * avg_30_day_call_volume THEN 0.5 ELSE 0 END +
      CASE WHEN call_ask_sweep_premium > call_bid_sweep_premium THEN 0.5 ELSE 0 END +
      CASE WHEN bullish_premium / NULLIF(bearish_premium, 0) > 2 THEN 0.5 ELSE 0 END +
      CASE WHEN call_volume_spike_percentile >= 0.6 THEN call_volume_spike_percentile ELSE 0 END +          		-- High call volume spike
      CASE WHEN call_ask_perc_percentile >= 0.6 THEN call_ask_perc_percentile ELSE 0 END +              			-- High call ask percentage
      CASE WHEN call_oi_delta_percentile >= 0.6 THEN call_oi_delta_percentile ELSE 0 END +              			-- Strong call open interest growth
      CASE WHEN call_oi_spike_percentile >= 0.6 THEN call_oi_spike_percentile ELSE 0 END +              			-- High call OI spike
      CASE WHEN call_sweep_volume_ratio_percentile >= 0.6 THEN call_sweep_volume_ratio_percentile ELSE 0 END +    -- High call sweep volume ratio
      CASE WHEN call_ask_sweep_ratio_percentile >= 0.6 THEN call_ask_sweep_ratio_percentile ELSE 0 END +       	-- High call ask sweep ratio
      CASE WHEN call_sweep_premium_ratio_percentile >= 0.6 THEN call_sweep_premium_ratio_percentile ELSE 0 END +  -- High call sweep premium ratio
      CASE WHEN put_volume_spike_percentile < 10 THEN 0.5 ELSE 0 END  												-- Low put volume spike (bull leaning)
    ) - (
      CASE WHEN put_volume_spike_30d > 2 THEN 0.5 ELSE 0 END +
      CASE WHEN put_ask_perc > 0.6 THEN 0.5 ELSE 0 END +
      CASE WHEN put_oi_delta_perc > 0.1 THEN 0.5 ELSE 0 END +
      CASE WHEN put_oi_spike_30d > 1.5 THEN 0.5 ELSE 0 END +
      CASE WHEN net_put_premium > 0 THEN 0.5 ELSE 0 END +
      CASE WHEN put_sweep_volume / NULLIF(put_volume, 0) > 0.3 THEN 0.5 ELSE 0 END +
      CASE WHEN round(sp.put_ask_sweep_volume / NULLIF(sp.put_sweep_volume, 0), 4) > 0.7 THEN 0.5 ELSE 0 END +
      CASE WHEN call_volume_spike_30d < 1 THEN 0.5 ELSE 0 END +
      CASE WHEN put_sweep_premium / NULLIF(put_premium, 0) > 0.5 THEN 0.5 ELSE 0 END +
      CASE WHEN put_sweep_volume > 2 * avg_30_day_put_volume THEN 0.5 ELSE 0 END +
      CASE WHEN put_ask_sweep_premium > put_bid_sweep_premium THEN 0.5 ELSE 0 END +
      CASE WHEN bearish_premium / NULLIF(bullish_premium, 0) > 2 THEN 0.5 ELSE 0 END +
      CASE WHEN put_volume_spike_percentile >= 0.6 THEN put_volume_spike_percentile ELSE 0 END +           		-- High put volume spike
      CASE WHEN put_ask_perc_percentile >= 0.6 THEN put_ask_perc_percentile ELSE 0 END +               			-- High put ask percentage
      CASE WHEN put_oi_delta_percentile >= 0.6 THEN put_oi_delta_percentile ELSE 0 END +               			-- Strong put open interest growth
      CASE WHEN put_oi_spike_percentile >= 0.6 THEN put_oi_spike_percentile ELSE 0 END +               			-- High put OI spike
      CASE WHEN put_sweep_volume_ratio_percentile >= 0.6 THEN put_sweep_volume_ratio_percentile ELSE 0 END +     	-- High put sweep volume ratio
      CASE WHEN put_ask_sweep_ratio_percentile >= 0.6 THEN put_ask_sweep_ratio_percentile ELSE 0 END +        	-- High put ask sweep ratio
      CASE WHEN put_sweep_premium_ratio_percentile >= 0.6 THEN put_sweep_premium_ratio_percentile ELSE 0 END +    -- High put sweep premium ratio
      CASE WHEN call_volume_spike_percentile < 10 THEN 0.5 ELSE 0 END            									-- Low call volume spike (bear leaning)
    ), 4) AS options_composite_score,
	os.file_version_date,
from optionscreener os
inner join sweep_and_prem_data sp
on sp.symbol = os.ticker
and sp.file_version_date = os.file_version_date
inner join symbol_sector ss
on ss.symbol = os.ticker
where 1 = 1
and os.file_version_date = '20250224'
--and sp.symbol = 'CELH'
order by os.file_version_date;