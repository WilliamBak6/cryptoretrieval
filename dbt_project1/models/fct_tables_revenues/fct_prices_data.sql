{{ config(
    materialized = 'table', 
    schema = 'fct_tables_revenues'
    ) 
}}

select
    *
from {{ ref('stg_prices_data') }}