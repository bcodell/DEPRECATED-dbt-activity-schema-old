{%- macro aggfunc_notnull(column) -%}
    {{ adapter.dispatch('aggfunc_notnull', 'dbt_activity_schema')(column) }}
{%- endmacro -%}

{%- macro default__aggfunc_notnull(column) -%}
max({{column}} is not null)
{%- endmacro -%}


