{%- macro aggfunc_listagg(column) -%}
    {{ adapter.dispatch('aggfunc_listagg', 'dbt_activity_schema')(column) }}
{%- endmacro -%}

{%- macro default__aggfunc_listagg(column) -%}
listagg({{column}}, '\n')
{%- endmacro -%}

{%- macro snowflake__aggfunc_listagg(column) -%}
listagg({{column}}, '\n')
{%- endmacro -%}

{%- macro bigquery__aggfunc_listagg(column) -%}
string_agg({{column}}, '\n')
{%- endmacro -%}

{%- macro redshift__aggfunc_listagg(column) -%}
list_agg({{column}}, '\n')
{%- endmacro -%}

{%- macro postgres__aggfunc_listagg(column) -%}
string_agg({{column}}, '\n')
{%- endmacro -%}
