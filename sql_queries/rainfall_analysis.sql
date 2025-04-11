/* Rainfall-based Analysis */

-- 1. Monthly/Yearly total rainfall by city

/* excluding 2024 as the data is either not accurate or null */
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
    SUM(rainfall_in_mm_cleaned) as rainfall
  from
    azzi_schema.all_cities_temp_rains_cleaned_final
  group by
    1,
    2,
    3
  order by
    2 DESC,
    3 desc
)

select
  A.*
from
  A
where
  year <> 2024


-- 2. Days with extreme rainfall events

-- Step 1: Calculate the 90th percentile rainfall per city

WITH rainfall_percentiles AS (
  SELECT
    city,
    PERCENTILE_CONT(0.9) WITHIN GROUP (
      ORDER BY
        rainfall_in_mm_cleaned
    ) AS rain_90
  FROM
    azzi_schema.all_cities_temp_rains_cleaned_final
  GROUP BY
    city
),

-- Step 2: Identify days where rainfall ≥ 90th percentile

extreme_rain_days AS (
  SELECT
    a.city,
    a.date_cleaned,
    a.rainfall_in_mm_cleaned,
    b.rain_90
  FROM
    azzi_schema.all_cities_temp_rains_cleaned_final a
    JOIN rainfall_percentiles b ON a.city = b.city
  WHERE
    a.rainfall_in_mm_cleaned >= b.rain_90
) 

-- Final result

SELECT
  *
FROM
  extreme_rain_days
ORDER BY
  city,
  date_cleaned;




--3. Compare Rainy Days vs Dry Days per City

-- Classifies each day based on rainfall and counts totals by category --

with day_classify as(
  select
    case
      when rainfall_in_mm_cleaned = 0 then 'dry'
      else 'rainy'
    end as day_classify,
    date_cleaned,
    city
  from
    azzi_schema.all_cities_temp_rains_cleaned_final
) 

-- Count of dry vs rainy days per city --

select
  city,
  day_classify,
  count(*) as total_days
from
  day_classify
group by
  1,
  2
order by
  1,
  2


-- 4. Longest Continuous Dry/Wet Streak per City --

-- Classifies each day and groups consecutive dates into streaks --

with day_classify as(
  select
    rainfall_in_mm_cleaned,
    case
      when rainfall_in_mm_cleaned = 0 then 'dry'
      else 'rainy'
    end as day_classify,
    date_cleaned,
    city
  from
    azzi_schema.all_cities_temp_rains_cleaned_final
),

-- Streak grouping logic: same city + day type + sequential dates --

grouped as 
(
  select
    *,
    date_cleaned - interval '1 day' * ROW_NUMBER() OVER(partition by city,day_classify order by date_cleaned ) as grp
  from
    day_classify
) 

-- Final result: Longest dry/wet streaks with start and end dates --

Select
  city,
  grp,
  day_classify,
  count(*) as streak_length,
  MIN(date_cleaned) AS start_date,
  MAX(date_cleaned) AS end_date
from
  grouped
group by
  1,
  2,
  3
order by
  streak_length DESC