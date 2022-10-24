{%- macro aggfunc_sum_bool(column) -%}
    {{ adapter.dispatch('aggfunc_sum_bool', 'dbt_activity_schema')(column) }}
{%- endmacro -%}

{%- macro default__aggfunc_sum_bool(column) -%}
sum({{column}}::int)
{%- endmacro -%}

{%- macro bigquery__aggfunc_sum_bool(column) -%}
sum(cast({{column}} as int64))
{%- endmacro -%}
