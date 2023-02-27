{%- macro aggfunc_count_distinct(column) -%}
    {{ adapter.dispatch('aggfunc_count_distinct', 'dbt_activity_schema')(column) }}
{%- endmacro -%}

{%- macro default__aggfunc_count_distinct(column) -%}
count(distinct {{column}})
{%- endmacro -%}
