{{ config(materialized="table") }}

WITH source AS (
    SELECT
        order_purchase_timestamp,
        EXTRACT(YEAR FROM order_purchase_timestamp) AS year_of_purchase
    FROM
        {{ source('warehouse', 'fact_sales') }}
    WHERE
        EXTRACT(YEAR FROM order_purchase_timestamp) IN (2017, 2018)
)

SELECT
    EXTRACT(MONTH FROM order_purchase_timestamp) AS order_purchase_month,
    SUM(
        CASE
            WHEN year_of_purchase = 2018 THEN 1
            ELSE 0.0
        END
    ) AS count_of_2018,
    SUM(
        CASE
            WHEN year_of_purchase = 2017 THEN 1
            ELSE 0.0
        END
    ) AS count_of_2017
FROM
    source
GROUP BY
    EXTRACT(MONTH FROM order_purchase_timestamp)
ORDER BY
    EXTRACT(MONTH FROM order_purchase_timestamp) ASC
