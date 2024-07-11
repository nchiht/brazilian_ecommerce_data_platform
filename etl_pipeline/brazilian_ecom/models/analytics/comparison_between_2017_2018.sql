{{ config(materialized="table") }}

WITH source AS (
    SELECT
        order_purchase_timestamp,
        EXTRACT(YEAR FROM TO_TIMESTAMP(order_purchase_timestamp, 'YYYY-MM-DD HH24:MI:SS.US')) AS year_of_purchase
    FROM
        {{ source('warehouse', 'fact_sales') }}
    WHERE
        EXTRACT(YEAR FROM TO_TIMESTAMP(order_purchase_timestamp, 'YYYY-MM-DD HH24:MI:SS.US')) IN (2017, 2018)
)

SELECT
    EXTRACT(MONTH FROM TO_TIMESTAMP(order_purchase_timestamp, 'YYYY-MM-DD HH24:MI:SS.US')) AS order_purchase_month,
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
    EXTRACT(MONTH FROM TO_TIMESTAMP(order_purchase_timestamp, 'YYYY-MM-DD HH24:MI:SS.US'))
ORDER BY
    EXTRACT(MONTH FROM TO_TIMESTAMP(order_purchase_timestamp, 'YYYY-MM-DD HH24:MI:SS.US')) ASC
