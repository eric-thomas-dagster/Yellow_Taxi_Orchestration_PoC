with zones as (

    select * from {{ ref('stg_taxi_zone_lookup') }}

),

enriched as (

    select
        location_id,
        borough,
        zone_name,
        service_zone,

        -- analytical flags
        case
            when service_zone = 'Airports' then true
            when zone_name ilike '%airport%' then true
            else false
        end                                     as is_airport,

        case
            when service_zone = 'EWR' then true
            else false
        end                                     as is_ewr,

        case
            when service_zone = 'Yellow Zone' then true
            else false
        end                                     as is_yellow_zone,

        -- borough grouping
        case
            when borough = 'Manhattan'                                          then 'Manhattan'
            when borough in ('Brooklyn', 'Queens', 'Bronx', 'Staten Island')   then 'Outer Borough'
            when borough = 'EWR'                                                then 'New Jersey'
            else 'Other'
        end                                     as borough_group,

        -- manhattan sub-regions for deeper spatial analysis
        case
            when borough = 'Manhattan' and (
                zone_name ilike '%Midtown%' or zone_name ilike '%Times Sq%'
                or zone_name ilike '%Penn Station%' or zone_name ilike '%Garment%'
            ) then 'Midtown'
            when borough = 'Manhattan' and (
                zone_name ilike '%Financial%' or zone_name ilike '%TriBeCa%'
                or zone_name ilike '%Battery Park%' or zone_name ilike '%Seaport%'
                or zone_name ilike '%World Trade%'
            ) then 'Lower Manhattan'
            when borough = 'Manhattan' and (
                zone_name ilike '%Upper East%' or zone_name ilike '%Upper West%'
                or zone_name ilike '%Lenox Hill%' or zone_name ilike '%Yorkville%'
                or zone_name ilike '%Lincoln Square%' or zone_name ilike '%Central Park%'
            ) then 'Upper Manhattan'
            when borough = 'Manhattan' and (
                zone_name ilike '%Harlem%' or zone_name ilike '%Washington Heights%'
                or zone_name ilike '%Inwood%' or zone_name ilike '%Hamilton%'
                or zone_name ilike '%Manhattanville%'
            ) then 'Northern Manhattan'
            when borough = 'Manhattan' then 'Other Manhattan'
            else null
        end                                     as manhattan_subregion

    from zones

)

select * from enriched
