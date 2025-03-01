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
    4;