/* Temperature-based Analysis */

-- 1. Monthly/Yearly average max/min temperature per city --


-- Calculates the monthly average of maximum and minimum temperatures per city
-- Excludes the year 2024 from the final result

with A as(
  select
    distinct(city) as city_,
    Extract(
      YEAR
      from
        date_cleaned
    ) as year,
    EXTRACT(
      MONTH
      FROM
        date_cleaned
    ) AS month,
    avg(max_temp_cleaned) as max_avg,
    avg(min_temp_cleaned) as min_avg
  from
    azzi_schema.all_cities_temp_rains_cleaned_final
  group by
    1,2,3
  order by
    2 DESC,3 DESC
)

select
  A.*
from
  A
where
  year <> 2024


  -- 2. Hottest and coldest day of the cities

  -- Hottest City

with T as (
  select
    city,
    dense_rank() over (
      partition by city
      order by
        max_temp_cleaned DESC
    ) as rn,
    max_temp_cleaned
  from
    azzi_schema.all_cities_temp_rains_cleaned_final
)
select
  DISTINCT(T.city) as unique_city,
  ROUND(T.max_temp_cleaned:: NUMERIC, 2) as rounded_max
from
  T
where
  rn = 1

  -- Coldest City

with T as (select 
city,
ROW_NUMBER() over (partition by city order by min_temp_cleaned) as rn,
min_temp_cleaned
from
azzi_schema.all_cities_temp_rains_cleaned_final
WHERE min_temp_cleaned > 0) /* as we had converted null/blanks to 0 during data cleansing part, using '> 0' condition to remove incorrect output */

select DISTINCT(T.city) as unique_city,
ROUND(T.min_temp_cleaned:: NUMERIC,2) as rounded_min
from
T
where rn = 1

--3. Detect heatwaves (e.g. 3+ consecutive days with high temp)


WITH city_temp_stats AS (                         --1st CTE
  SELECT
    city,
    PERCENTILE_CONT(0.9) WITHIN GROUP (
      ORDER BY
        max_temp_cleaned
    ) AS temp_90
  FROM
    azzi_schema.all_cities_temp_rains_cleaned_final
  GROUP BY
    city
),

hot_days AS (                                     --2nd CTE
  SELECT
    a.city,
    a.date_cleaned,
    a.max_temp_cleaned,
    CASE
      WHEN a.max_temp_cleaned >= b.temp_90 THEN 1
      ELSE 0
    END AS is_hot
  FROM
    azzi_schema.all_cities_temp_rains_cleaned_final a
    JOIN city_temp_stats b ON a.city = b.city
),

consec_groups AS (                                --3rd CTE
  SELECT
    *,
    date_cleaned - INTERVAL '1 day' * ROW_NUMBER() OVER (
      PARTITION BY city
      ORDER BY
        date_cleaned
    ) AS grp
  FROM
    hot_days
  WHERE
    is_hot = 1
),

grouped AS (                                      --4rd CTE
  SELECT
    city,
    grp,
    COUNT(*) AS streak_length,
    MIN(date_cleaned) AS start_date,
    MAX(date_cleaned) AS end_date
  FROM
    consec_groups
  GROUP BY
    city,
    grp
)

SELECT                                            -- final select
  *
FROM
  grouped
WHERE
  streak_length >= 3
ORDER BY
  streak_length DESC


-- 4. Temperature deviation

-- Calculates daily temperature deviation across cities by finding the difference between 
-- the highest and lowest maximum temperatures recorded on each date

WITH temp_extremes AS (
  SELECT 
    date_cleaned,
    MAX(max_temp_cleaned) AS max_temp_across_cities,
    MIN(max_temp_cleaned) AS min_temp_across_cities
  FROM azzi_schema.all_cities_temp_rains_cleaned_final
  GROUP BY date_cleaned
),
temp_deviation AS (
  SELECT 
    date_cleaned,
    ROUND((max_temp_across_cities - min_temp_across_cities)::numeric, 2) AS temp_deviation
  FROM temp_extremes
)
SELECT *
FROM temp_deviation
ORDER BY temp_deviation DESC;