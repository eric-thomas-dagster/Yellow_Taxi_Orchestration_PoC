{#
    Cross-database compatibility macros.
    These allow West Bend's Snowflake-native SQL to run on DuckDB for local demos.
    In production Snowflake, these are never invoked — the native SQL syntax is used directly.

    NOTE: These macros are ONLY needed for local DuckDB demo mode.
    West Bend's actual Snowflake SQL is unchanged.
#}

{% macro date_from_parts(year, month, day) %}
    {% if target.type == 'duckdb' %}
        make_date({{ year }}, {{ month }}, {{ day }})
    {% else %}
        date_from_parts({{ year }}, {{ month }}, {{ day }})
    {% endif %}
{% endmacro %}
