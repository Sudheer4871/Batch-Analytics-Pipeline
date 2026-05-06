-- mart_weather__daily_summary.sql
-- Daily aggregated weather metrics per location
-- Grain: one row per (date, latitude, longitude)

with hourly as (

    select * from {{ ref('stg_meteo__hourly') }}

),

daily_agg as (

    select
        -- grain
        observed_date,
        latitude,
        longitude,

        -- temperature summary
        round(avg(temperature_c), 2)            as avg_temp_c,
        round(min(temperature_c), 2)            as min_temp_c,
        round(max(temperature_c), 2)            as max_temp_c,
        round(max(temperature_c)
              - min(temperature_c), 2)          as temp_range_c,

        round(avg(temperature_f), 2)            as avg_temp_f,
        round(min(temperature_f), 2)            as min_temp_f,
        round(max(temperature_f), 2)            as max_temp_f,

        -- humidity
        round(avg(relative_humidity_pct), 2)    as avg_humidity_pct,
        round(min(relative_humidity_pct), 2)    as min_humidity_pct,
        round(max(relative_humidity_pct), 2)    as max_humidity_pct,

        -- precipitation
        round(sum(precipitation_mm), 2)         as total_precipitation_mm,
        count(case when precipitation_mm > 0
              then 1 end)                       as rainy_hours_count,
        round(max(precipitation_mm), 2)         as peak_hourly_precipitation_mm,

        -- wind
        round(avg(wind_speed_kmh), 2)           as avg_wind_speed_kmh,
        round(max(wind_speed_kmh), 2)           as max_wind_speed_kmh,
        round(avg(wind_speed_mph), 2)           as avg_wind_speed_mph,
        round(max(wind_speed_mph), 2)           as max_wind_speed_mph,

        -- dominant weather condition (mode by hour count)
        mode(weather_description)               as dominant_weather_condition,

        -- day classification
        case
            when sum(precipitation_mm) = 0      then 'Dry'
            when sum(precipitation_mm) < 5      then 'Light Rain'
            when sum(precipitation_mm) < 20     then 'Moderate Rain'
            else 'Heavy Rain'
        end                                     as precipitation_category,

        case
            when max(temperature_c) >= 35       then 'Extreme Heat'
            when max(temperature_c) >= 30       then 'Hot'
            when max(temperature_c) >= 20       then 'Warm'
            when max(temperature_c) >= 10       then 'Mild'
            when max(temperature_c) >= 0        then 'Cold'
            else 'Freezing'
        end                                     as temperature_category,

        -- data completeness
        count(*)                                as hours_recorded,
        count(*) = 24                           as is_full_day,

        -- metadata
        max(loaded_at)                          as last_loaded_at

    from hourly
    group by 1, 2, 3

)

select * from daily_agg