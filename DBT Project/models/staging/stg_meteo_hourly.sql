-- stg_meteo__hourly.sql
-- Cleans and types raw hourly weather data from Open-Meteo API

with source as (

    select * from {{ source('meteo', 'hourly_weather') }}

),

renamed as (

    select
        -- timestamps
        cast(time as timestamp_ntz)                     as observed_at,
        cast(time as date)                              as observed_date,
        date_part('hour', cast(time as timestamp_ntz))  as observed_hour,

        -- location
        cast(latitude as float)                         as latitude,
        cast(longitude as float)                        as longitude,

        -- temperature & humidity
        cast(temperature_2m as float)                   as temperature_c,
        round(cast(temperature_2m as float) * 9/5 + 32, 2) as temperature_f,
        cast(relative_humidity_2m as float)             as relative_humidity_pct,

        -- precipitation
        cast(precipitation as float)                    as precipitation_mm,

        -- wind
        cast(wind_speed_10m as float)                   as wind_speed_kmh,
        round(cast(wind_speed_10m as float) / 1.609, 2) as wind_speed_mph,
        cast(wind_direction_10m as int)                 as wind_direction_deg,

        -- weather classification
        cast(weathercode as int)                        as weather_code,
        case
            when weathercode = 0                        then 'Clear Sky'
            when weathercode in (1, 2, 3)               then 'Partly Cloudy'
            when weathercode in (45, 48)                then 'Foggy'
            when weathercode in (51, 53, 55)            then 'Drizzle'
            when weathercode in (61, 63, 65)            then 'Rain'
            when weathercode in (71, 73, 75)            then 'Snow'
            when weathercode in (80, 81, 82)            then 'Rain Showers'
            when weathercode in (95, 96, 99)            then 'Thunderstorm'
            else 'Unknown'
        end                                             as weather_description,

        -- metadata
        cast(loaded_at as timestamp_ntz)                as loaded_at

    from source

)

select * from renamed