{% macro _get_relationships() %}

{%- set relationships = {
    'first_ever': dbt_activity_schema.join_first_ever,
    'last_ever': dbt_activity_schema.join_last_ever,
    'nth_ever': dbt_activity_schema.join_nth_ever,
    'first_before': dbt_activity_schema.join_first_before,
    'last_before': dbt_activity_schema.join_last_before,
    'first_after': dbt_activity_schema.join_first_after,
    'last_after': dbt_activity_schema.join_last_after,
    'first_in_between': dbt_activity_schema.join_first_in_between,
    'last_in_between': dbt_activity_schema.join_last_in_between,
    'aggregate_all_ever': dbt_activity_schema.join_aggregate_all_ever,
    'aggregate_before': dbt_activity_schema.join_aggregate_before,
    'aggregate_after': dbt_activity_schema.join_aggregate_after,
    'aggregate_in_between': dbt_activity_schema.join_aggregate_in_between,
    'custom': dbt_activity_schema.join_custom,
} -%}

{%- do return(relationships) -%}

{% endmacro %}