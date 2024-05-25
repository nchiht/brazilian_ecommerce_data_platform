{{ config(materialized="table") }}

WITH geolocation_data AS (
    SELECT
        geolocation_zip_code_prefix,
        geolocation_lat,
        geolocation_lng
    FROM
        {{ source('warehouse', 'dim_geolocation') }}
)

SELECT
  (
    FLOOR(
      (
        (geolocation_data.geolocation_lat - -23.8) / 0.1
      )
    ) * 0.1
  ) + -23.8 AS geolocation_lat_bucket,
  (
    FLOOR(
      (
        (geolocation_data.geolocation_lng - -46.7) / 0.1
      )
    ) * 0.1
  ) + -46.7 AS geolocation_lng_bucket,
  COUNT(*) AS count
FROM
  {{ source('warehouse', 'dim_customers') }} AS dim_customers
LEFT JOIN geolocation_data
  ON dim_customers.customer_zip_code_prefix = geolocation_data.geolocation_zip_code_prefix
GROUP BY
  geolocation_lat_bucket,
  geolocation_lng_bucket
ORDER BY
  geolocation_lat_bucket ASC,
  geolocation_lng_bucket ASC
