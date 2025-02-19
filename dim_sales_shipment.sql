{{config(materialized = 'table')}}

SELECT DISTINCT
    {{dbt_utils.generate_surrogate_key([
        '`ship-city`',
        '`ship-state`'
        ])}} AS shipment_id,
    `ship-service-level` AS ship_service_level,
    `ship-city` AS ship_city,
    `ship-state` AS ship_state,
    `ship-postal-code` AS ship_postal_code,
    `ship-country` AS ship_country
FROM {{source('bronze', 'amazon_sale_report')}}