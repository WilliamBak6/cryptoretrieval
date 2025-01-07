{{ config(
    materialized = 'table'
    ) 
}}

with big_table as (
    select
        *
    from {{ source('crytocurrencies', 'raw_prices_data') }}

    union

    select 
        *
    from {{ this }}
    where date not in (
        select distinct date from {{ source('crytocurrencies', 'raw_prices_data') }}
    )
)

select * from big_table