{{ config(materialized="table") }}

WITH geolocation_buckets AS (
    SELECT
        (
            FLOOR(
                (
                    (dim_sellers.geolocation_lng - -46.7) / 0.1
                )
            ) * 0.1
        ) + -46.7 AS geolocation_lng_bucket,
        (
            FLOOR(
                (
                    (dim_sellers.geolocation_lat - -23.8) / 0.1
                )
            ) * 0.1
        ) + -23.8 AS geolocation_lat_bucket,
        COUNT(*) AS count
    FROM
        {{ source('warehouse', 'dim_sellers') }} AS dim_sellers
    GROUP BY
        geolocation_lng_bucket,
        geolocation_lat_bucket
)

SELECT
    geolocation_lng_bucket AS geolocation_lng,
    geolocation_lat_bucket AS geolocation_lat,
    count
FROM
    geolocation_buckets
ORDER BY
    geolocation_lng ASC,
    geolocation_lat ASC
