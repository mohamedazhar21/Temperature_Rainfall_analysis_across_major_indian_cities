/* DATA PREPROCESSING & CLEANING */

-- Below are the example codes for one of the cities/tables



-- 1. Renaming column names

alter table azzi_schema.mumbai_temp_rains rename column "Date" to event_date;
alter table azzi_schema.mumbai_temp_rains rename column "Rain" to rainfall_in_mm;
alter table azzi_schema.mumbai_temp_rains rename column "Temp Max" to max_temp;
alter table azzi_schema.mumbai_temp_rains rename column "Temp Min" to min_temp;



-- 2. Data Cleaning(Handling Nulls, blanks and others)

/* Blanks ('') are first converted to NULL using the NULLIF function, then replaced with 0 using COALESCE. The data type is also cast to FLOAT in this step */


CREATE TABLE azzi_schema.chennai_temp_rains_cleaned AS (
  SELECT
    TO_DATE(event_date, 'DD-MM-YYYY') AS date_cleaned,
    COALESCE(NULLIF(rainfall_in_mm, ''):: FLOAT, 0) AS rainfall_in_mm_cleaned,
    COALESCE(max_temp, 0):: FLOAT AS max_temp_cleaned,
    COALESCE(min_temp, 0):: FLOAT AS min_temp_cleaned
  FROM
    azzi_schema.chennai_temp_rains
);


/* Since PostgreSQL cannot interpret Excel date formats directly, necessary transformations were applied to standardize the date columns in some files */

create table azzi_schema.hyd_temp_rains_cleaned as (
  select
    CASE
      WHEN event_date ~ '^\d{2}-\d{2}-\d{4}$' THEN TO_DATE(event_date, 'DD-MM-YYYY')
      WHEN event_date ~ '^\d+$' THEN DATE '1899-12-30' + event_date:: INT
      ELSE NULL
    END AS date_cleaned,
    COALESCE(rainfall_in_mm, 0):: FLOAT AS rainfall_in_mm_cleaned,
    COALESCE(max_temp, 0):: FLOAT AS max_temp_cleaned,
    COALESCE(min_temp, 0):: FLOAT AS min_temp_cleaned,
    'Hyderabad' as City
  from
    azzi_schema.hyd_temp_rains
); 



-- 3. Unified dataset for cross city analysis

CREATE TABLE azzi_schema.all_cities_temp_rains_cleaned_final AS
SELECT
  *
FROM
  azzi_schema.amd_temp_rains_cleaned_final
UNION ALL
SELECT
  *
FROM
  azzi_schema.mumbai_temp_rains_cleaned_final
UNION ALL
SELECT
  *
FROM
  azzi_schema.chennai_temp_rains_cleaned_final
UNION ALL
SELECT
  *
FROM
  azzi_schema.delhi_temp_rains_cleaned_final
UNION ALL
SELECT
  *
FROM
  azzi_schema.hyd_temp_rains_cleaned_final
UNION ALL
SELECT
  *
FROM
  azzi_schema.kolkata_temp_rains_cleaned_final
UNION ALL
SELECT
  *
FROM
  azzi_schema.bengaluru_temp_rains_cleaned_final;


  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------




