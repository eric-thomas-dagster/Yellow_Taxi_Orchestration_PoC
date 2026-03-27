{#
    Creates demo source tables in DuckDB that simulate what the Databricks
    ingestion notebooks would produce. Only runs when target is 'dev' (DuckDB).

    In production Snowflake, these tables are Iceberg external tables created
    by the Databricks ingestion notebooks and the EDL DDL scripts.

    Call with: dbt run-operation create_demo_sources
#}

{% macro create_demo_sources() %}
    {% set env = var('env', 'DEV') %}
    {% set schema_name = env ~ '_EDL' %}

    {% if target.type == 'duckdb' %}

        {% set create_schema %}
            create schema if not exists "{{ schema_name }}";
        {% endset %}
        {% do run_query(create_schema) %}

        {% set create_zones %}
            create or replace table "{{ schema_name }}"."TAXI_ZONE_LOOKUP" as
            select
                col0 as "LOCATIONID",
                col1 as "BOROUGH",
                col2 as "ZONE",
                col3 as "SERVICE_ZONE"
            from (values
                (1, 'EWR', 'Newark Airport', 'EWR'),
                (2, 'Queens', 'Jamaica Bay', 'Boro Zone'),
                (3, 'Bronx', 'Allerton/Pelham Gardens', 'Boro Zone'),
                (4, 'Manhattan', 'Alphabet City', 'Yellow Zone'),
                (12, 'Manhattan', 'Battery Park', 'Yellow Zone'),
                (13, 'Manhattan', 'Battery Park City', 'Yellow Zone'),
                (43, 'Manhattan', 'Central Park', 'Yellow Zone'),
                (48, 'Manhattan', 'Clinton East', 'Yellow Zone'),
                (50, 'Manhattan', 'Clinton West', 'Yellow Zone'),
                (68, 'Manhattan', 'East Chelsea', 'Yellow Zone'),
                (79, 'Manhattan', 'East Village', 'Yellow Zone'),
                (87, 'Manhattan', 'Financial District North', 'Yellow Zone'),
                (88, 'Manhattan', 'Financial District South', 'Yellow Zone'),
                (90, 'Manhattan', 'Flatiron', 'Yellow Zone'),
                (100, 'Manhattan', 'Garment District', 'Yellow Zone'),
                (107, 'Manhattan', 'Gramercy', 'Yellow Zone'),
                (113, 'Manhattan', 'Greenwich Village North', 'Yellow Zone'),
                (114, 'Manhattan', 'Greenwich Village South', 'Yellow Zone'),
                (125, 'Manhattan', 'Hudson Sq', 'Yellow Zone'),
                (128, 'Queens', 'JFK Airport', 'Airports'),
                (132, 'Queens', 'JFK Airport', 'Airports'),
                (138, 'Manhattan', 'LaGuardia Airport', 'Airports'),
                (140, 'Manhattan', 'Lenox Hill East', 'Yellow Zone'),
                (141, 'Manhattan', 'Lenox Hill West', 'Yellow Zone'),
                (142, 'Manhattan', 'Lincoln Square East', 'Yellow Zone'),
                (143, 'Manhattan', 'Lincoln Square West', 'Yellow Zone'),
                (144, 'Manhattan', 'Little Italy/NoLiTa', 'Yellow Zone'),
                (148, 'Manhattan', 'Lower East Side', 'Yellow Zone'),
                (151, 'Manhattan', 'Manhattan Valley', 'Yellow Zone'),
                (152, 'Manhattan', 'Manhattanville', 'Yellow Zone'),
                (158, 'Manhattan', 'Meatpacking/West Village West', 'Yellow Zone'),
                (161, 'Manhattan', 'Midtown Center', 'Yellow Zone'),
                (162, 'Manhattan', 'Midtown East', 'Yellow Zone'),
                (163, 'Manhattan', 'Midtown North', 'Yellow Zone'),
                (164, 'Manhattan', 'Midtown South', 'Yellow Zone'),
                (166, 'Manhattan', 'Morningside Heights', 'Yellow Zone'),
                (170, 'Manhattan', 'Murray Hill', 'Yellow Zone'),
                (186, 'Manhattan', 'Penn Station/Madison Sq West', 'Yellow Zone'),
                (194, 'Manhattan', 'Randalls Island', 'Yellow Zone'),
                (202, 'Manhattan', 'Roosevelt Island', 'Yellow Zone'),
                (209, 'Manhattan', 'Seaport', 'Yellow Zone'),
                (211, 'Manhattan', 'SoHo', 'Yellow Zone'),
                (224, 'Manhattan', 'Stuy Town/PCV', 'Yellow Zone'),
                (229, 'Manhattan', 'Sutton Place/Turtle Bay North', 'Yellow Zone'),
                (230, 'Manhattan', 'Sutton Place/Turtle Bay South', 'Yellow Zone'),
                (231, 'Manhattan', 'Times Sq/Theatre District', 'Yellow Zone'),
                (232, 'Manhattan', 'TriBeCa/Civic Center', 'Yellow Zone'),
                (233, 'Manhattan', 'Two Bridges/Seward Park', 'Yellow Zone'),
                (234, 'Manhattan', 'UN/Turtle Bay South', 'Yellow Zone'),
                (236, 'Manhattan', 'Upper East Side North', 'Yellow Zone'),
                (237, 'Manhattan', 'Upper East Side South', 'Yellow Zone'),
                (238, 'Manhattan', 'Upper West Side North', 'Yellow Zone'),
                (239, 'Manhattan', 'Upper West Side South', 'Yellow Zone'),
                (243, 'Manhattan', 'Washington Heights North', 'Yellow Zone'),
                (244, 'Manhattan', 'Washington Heights South', 'Yellow Zone'),
                (246, 'Manhattan', 'West Chelsea/Hudson Yards', 'Yellow Zone'),
                (249, 'Manhattan', 'West Village', 'Yellow Zone'),
                (261, 'Manhattan', 'World Trade Center', 'Yellow Zone'),
                (262, 'Manhattan', 'Yorkville East', 'Yellow Zone'),
                (263, 'Manhattan', 'Yorkville West', 'Yellow Zone'),
                (264, 'Unknown', 'NV', 'N/A'),
                (265, 'Unknown', 'NA', 'N/A'),
                (61, 'Brooklyn', 'Crown Heights North', 'Boro Zone'),
                (97, 'Brooklyn', 'Fort Greene', 'Boro Zone'),
                (181, 'Brooklyn', 'Park Slope', 'Boro Zone'),
                (188, 'Brooklyn', 'Prospect Heights', 'Boro Zone'),
                (255, 'Brooklyn', 'Williamsburg (North Side)', 'Boro Zone'),
                (256, 'Brooklyn', 'Williamsburg (South Side)', 'Boro Zone'),
                (17, 'Bronx', 'Bedford Park', 'Boro Zone'),
                (119, 'Manhattan', 'Hamilton Heights', 'Yellow Zone'),
                (116, 'Manhattan', 'Harlem', 'Yellow Zone'),
                (41, 'Manhattan', 'Central Harlem', 'Yellow Zone'),
                (42, 'Manhattan', 'Central Harlem North', 'Yellow Zone'),
                (120, 'Manhattan', 'Highbridge Park', 'Yellow Zone'),
                (127, 'Manhattan', 'Inwood', 'Yellow Zone'),
                (153, 'Manhattan', 'Marble Hill', 'Yellow Zone')
            );
        {% endset %}
        {% do run_query(create_zones) %}

        {% set create_trips %}
            create or replace table "{{ schema_name }}"."YELLOW_TAXI_TRIPS" as
            select * from (
                -- Generate ~100 sample trips across a few months
                select
                    (case when random() < 0.6 then 1 when random() < 0.8 then 2 else 6 end) as "VENDORID",
                    timestamp '2023-01-15 08:30:00' + interval (i * 47) minute
                        + interval (case when i % 7 = 0 then 720 when i % 5 = 0 then 1440 else 0 end) hour
                        as "TPEP_PICKUP_DATETIME",
                    timestamp '2023-01-15 08:30:00' + interval (i * 47) minute
                        + interval (case when i % 7 = 0 then 720 when i % 5 = 0 then 1440 else 0 end) hour
                        + interval (10 + (i % 30) * 2) minute
                        as "TPEP_DROPOFF_DATETIME",
                    (1 + (i % 4)) as "PASSENGER_COUNT",
                    round(1.0 + (i % 15) * 0.8, 2) as "TRIP_DISTANCE",
                    case i % 10
                        when 0 then 161 when 1 then 162 when 2 then 236
                        when 3 then 237 when 4 then 170 when 5 then 100
                        when 6 then 48  when 7 then 138 when 8 then 132
                        else 4
                    end as "PULOCATIONID",
                    case (i + 3) % 10
                        when 0 then 239 when 1 then 238 when 2 then 142
                        when 3 then 107 when 4 then 163 when 5 then 231
                        when 6 then 211 when 7 then 87  when 8 then 1
                        else 113
                    end as "DOLOCATIONID",
                    (case when i % 20 = 0 then 2 when i % 15 = 0 then 5 else 1 end) as "RATECODEID",
                    (case when i % 12 = 0 then 'Y' else 'N' end) as "STORE_AND_FWD_FLAG",
                    (case when i % 3 = 0 then 2 when i % 5 = 0 then 0 else 1 end) as "PAYMENT_TYPE",
                    round(7.0 + (i % 20) * 2.5, 2) as "FARE_AMOUNT",
                    round((i % 3) * 0.5, 2) as "EXTRA",
                    0.50 as "MTA_TAX",
                    round(case when i % 3 != 0 then (7.0 + (i % 20) * 2.5) * 0.18 else 0 end, 2) as "TIP_AMOUNT",
                    round(case when i % 8 = 0 then 6.55 else 0 end, 2) as "TOLLS_AMOUNT",
                    0.30 as "IMPROVEMENT_SURCHARGE",
                    2.50 as "CONGESTION_SURCHARGE",
                    round(case when i % 10 in (7, 8) then 1.25 else 0 end, 2) as "AIRPORT_FEE",
                    round(7.0 + (i % 20) * 2.5 + (i % 3) * 0.5 + 0.50
                          + case when i % 3 != 0 then (7.0 + (i % 20) * 2.5) * 0.18 else 0 end
                          + case when i % 8 = 0 then 6.55 else 0 end
                          + 0.30 + 2.50
                          + case when i % 10 in (7, 8) then 1.25 else 0 end, 2) as "TOTAL_AMOUNT"
                from generate_series(0, 99) as t(i)
            );
        {% endset %}
        {% do run_query(create_trips) %}

        {{ log("Demo source tables created in schema " ~ schema_name, info=True) }}

    {% else %}
        {{ log("Skipping demo source creation — not running on DuckDB target", info=True) }}
    {% endif %}
{% endmacro %}
