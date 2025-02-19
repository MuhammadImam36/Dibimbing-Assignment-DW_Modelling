{{config(materialized = 'table')}}

SELECT DISTINCT
    {{dbt_utils.generate_surrogate_key([
        '`Sales Channel `'])}} AS channel_id,
    `Sales Channel ` AS sales_channel
FROM {{source('bronze', 'amazon_sale_report')}}