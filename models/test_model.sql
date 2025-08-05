{# {{ config(materialized='table') }}

SELECT 
    CURRENT_DATE AS today_date,
    CURRENT_TIMESTAMP AS current_time,
    '{{ target.name }}' AS dbt_target,
    '{{ target.schema }}' AS dbt_schema,
    '{{ target.database }}' AS dbt_database #}


{{ config(materialized='table') }}

SELECT * FROM VALUES
    (1, 'Alice'),
    (2, 'Bob'),
    (3, 'Charlie')
AS t(id, name)
