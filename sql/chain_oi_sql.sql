select oc.*,
rank() over (partition by file_version_date order by unusual_chain_count_perc desc, unusual_chain_count desc, unusual_chain_vol desc, chain_volume_perctl desc, chain_vol_spike desc) as unusual_chain_vol_rank
FROM(
	select 
		underlying_symbol,
		option_symbol,
		chain_expiry_date,
		option_type,
		strike,
		curr_oi,
		last_oi,
		oi_diff_plain,
		oi_change_perc,
		avg_price,
		curr_chain_vol,
		prev_chain_vol,
		change_chain_vol,
		round((curr_chain_vol-prev_chain_vol)/nullif(prev_chain_vol, 0), 4) as chain_vol_spike,
		round(coalesce(curr_chain_vol /nullif(curr_oi, 0), curr_chain_vol), 4) as unusual_chain_vol,
		sum(case when curr_chain_vol >= 1000 and unusual_chain_vol >= 1 then 1 end) over (partition by underlying_symbol, file_version_date) as unusual_chain_count,
		count(distinct option_symbol) over (partition by underlying_symbol, file_version_date) as tot_chain_count,
		round(unusual_chain_count/nullif(tot_chain_count, 0), 4) as unusual_chain_count_perc,
		round(percent_rank() over (partition by underlying_symbol, file_version_date order by change_chain_vol), 4) as chain_volume_perctl,
		rank() over (partition by underlying_symbol, file_version_date order by oi_change_perc desc, curr_chain_vol desc, avg_price desc) as chain_oi_rank,
		file_version_date
	from 
	(
		select 
			option_symbol,
			underlying_symbol,
			try_cast('20'||substring(option_symbol, length(underlying_symbol)+1, 6) as int) as chain_expiry_date,
			substring(option_symbol, length(underlying_symbol)+7, 1) as option_type,
			try_cast(strike as double) as strike,
			try_cast(curr_oi as bigint) as curr_oi,
			try_cast(last_oi as bigint) as last_oi,
			try_cast(oi_diff_plain as bigint) as oi_diff_plain,
			round(try_cast(oi_change as double), 4) as oi_change_perc,
			try_cast(avg_price as double) as avg_price,
			try_cast(curr_vol as bigint) as curr_chain_vol,
			try_cast(prev_vol as bigint) as prev_chain_vol,
			try_cast(curr_vol as bigint)-try_cast(prev_vol as bigint) as change_chain_vol,
			file_version_date
		from read_parquet("R:\local_bucket\raw_store\whales\oichanges\parquet\*\*.parquet", hive_partitioning = True) CC
		where file_version_date = '20250226'
		--and underlying_symbol in('CEG')
	) oc
) oc
order by unusual_stock_vol_rank;


WITH parsed_chains AS (
    -- Parse and clean raw data
SELECT 
        option_symbol,
        underlying_symbol,
        try_cast('20'||substring(option_symbol, length(underlying_symbol)+1, 6) as int) as chain_expiry_date,
        SUBSTRING(option_symbol, LENGTH(underlying_symbol) + 7, 1) AS option_type,
        try_cast(strike as double) as strike,
        TRY_CAST(REPLACE(curr_oi, ',', '') AS BIGINT) AS curr_oi,
        TRY_CAST(REPLACE(last_oi, ',', '') AS BIGINT) AS last_oi,
        TRY_CAST(oi_diff_plain AS BIGINT) AS oi_diff_plain,
        ROUND(TRY_CAST(oi_change AS DOUBLE), 4) AS oi_change_perc,
        TRY_CAST(avg_price AS DOUBLE) AS avg_price,
        TRY_CAST(curr_vol AS BIGINT) AS curr_chain_vol,
        TRY_CAST(prev_vol AS BIGINT) AS prev_chain_vol,
        TRY_CAST(curr_vol AS BIGINT) - TRY_CAST(prev_vol AS BIGINT) AS change_chain_vol,
        file_version_date
    FROM read_parquet('R:\local_bucket\raw_store\whales\oichanges\parquet\*\*.parquet', hive_partitioning = True)
    WHERE file_version_date = '20250226'
),
chain_metrics AS (
    -- Chain-level metrics and ranks
    SELECT 
        underlying_symbol,
        option_symbol,
        chain_expiry_date,
        option_type,
        strike,
        curr_oi,
        last_oi,
        oi_diff_plain,
        oi_change_perc,
        avg_price,
        curr_chain_vol,
        prev_chain_vol,
        change_chain_vol,
        ROUND((curr_chain_vol - prev_chain_vol) / NULLIF(prev_chain_vol, 0), 4) AS chain_vol_spike,
        ROUND(COALESCE(curr_chain_vol / NULLIF(curr_oi, 0), curr_chain_vol), 4) AS unusual_chain_vol,
        SUM(CASE WHEN curr_chain_vol >= 1000 AND unusual_chain_vol >= 1 THEN 1 ELSE 0 END) 
            OVER (PARTITION BY underlying_symbol, file_version_date) AS unusual_chain_count,
        CASE WHEN curr_chain_vol >= 1000 AND unusual_chain_vol >= 1 THEN 'Y' ELSE 'N' END as unusual_chain_indc,
        COUNT(DISTINCT option_symbol) 
            OVER (PARTITION BY underlying_symbol, file_version_date) AS tot_chain_count,
        ROUND(unusual_chain_count / NULLIF(tot_chain_count, 0), 4) AS unusual_chain_count_perc,
        ROUND(PERCENT_RANK() 
            OVER (PARTITION BY underlying_symbol, file_version_date ORDER BY change_chain_vol), 4) AS chain_volume_perctl,
        RANK() 
            OVER (PARTITION BY underlying_symbol, file_version_date ORDER BY oi_change_perc DESC, curr_chain_vol DESC, avg_price DESC) AS chain_oi_rank,
        file_version_date
    FROM parsed_chains
),
stock_metrics AS (
    -- Stock-level aggregates and ranking based on proportion of unusual activity
    SELECT 
        underlying_symbol,
        file_version_date,
        SUM(curr_oi) AS total_oi,
        SUM(curr_chain_vol) AS total_vol,
        SUM(ABS(oi_diff_plain)) AS total_oi_change,
        MAX(unusual_chain_count) AS total_unusual_chains,  -- Max because it's a windowed sum
        MAX(tot_chain_count) AS total_chains,
        MAX(unusual_chain_count_perc) AS unusual_chain_perc,  -- Max because it's constant per stock/date
        ROUND(SUM(curr_chain_vol) / NULLIF(MAX(tot_chain_count), 0), 2) AS avg_vol_per_chain,
        ROUND(SUM(ABS(oi_diff_plain)) / NULLIF(MAX(tot_chain_count), 0), 2) AS avg_oi_change_per_chain,
        RANK() OVER (PARTITION BY file_version_date 
                     ORDER BY unusual_chain_perc DESC, 
                              avg_vol_per_chain DESC, 
                              avg_oi_change_per_chain DESC) AS stock_rank
    FROM chain_metrics
    GROUP BY underlying_symbol, file_version_date
),
final_summary AS (
    -- Combine chain and stock data with rankings
    SELECT 
        c.*,
        s.stock_rank,
        s.unusual_chain_perc AS stock_unusual_chain_perc,
        s.avg_vol_per_chain AS stock_avg_vol_per_chain,
        s.avg_oi_change_per_chain AS stock_avg_oi_change_per_chain,
        case when unusual_chain_indc = 'Y' then RANK() OVER (PARTITION BY c.file_version_date 
                     ORDER BY c.unusual_chain_count_perc DESC, c.unusual_chain_count DESC, 
                              c.unusual_chain_vol DESC, c.chain_volume_perctl DESC, c.chain_vol_spike DESC nulls last) end AS unusual_chain_vol_rank
    FROM chain_metrics c
    JOIN stock_metrics s 
        ON c.underlying_symbol = s.underlying_symbol 
        AND c.file_version_date = s.file_version_date
)
SELECT *
FROM final_summary
ORDER BY unusual_chain_vol_rank nulls last;