{{ config(
    materialized = 'view', 
    schema = 'stg_tables_revenues'
    ) 
}}

with big_table as (
    select
        *
    from {{ ref('int_prices_data') }}
),

t_manipulation as (
    select
        priceUsd as price,
        str_to_date(replace(replace(date, ".000Z", ""), "T00:00:00", ""), "%Y-%m-%d")               as date,
        id                                                                                          as id

    from big_table
),

t2_manipulation as (
    select
        price,
        case 
            when year(current_date()) = year(date) then 'y0'
            when year(current_date()) - 1 = year(date) then 'y1'
            when year(current_date()) - 2 = year(date) then 'y2'
            when year(current_date()) - 3 = year(date) then 'y3'
            else NULL
        end                                                                                         as year,
        date,
        id
    
    from t_manipulation

),

pivoted_data as (
    select
        id,
        case
            when year(current_date()) = year(date) then date 
            when year(current_date()) - 1 = year(date) then date_add(date, interval 1 year)
            when year(current_date()) - 2 = year(date) then date_add(date, interval 2 year)
            when year(current_date()) - 3 = year(date) then date_add(date, interval 3 year)
            else NULL
        end                                                                                     as new_date,
        case
            when year(current_date()) = year(date) then price
            else NULL
        end                                                                                     as y0,
        case
            when year(current_date()) - 1 = year(date) then price
            else NULL
        end                                                                                     as y1,
        case
            when year(current_date()) - 2 = year(date) then price
            else NULL
        end                                                                                     as y2,
        case
            when year(current_date()) - 3 = year(date) then price
            else NULL
        end                                                                                     as y3 -- Those year represents the price in those previois year
        -- The number after y is the number of years back
    
    from t2_manipulation
), 

_final as (
    select 
        id,
        new_date,
        sum(y0)                             as y0,
        sum(y1)                             as y1,
        sum(y2)                             as y2,
        sum(y3)                             as y3
    
    from pivoted_data
    group by id, new_date
)

select * from _final