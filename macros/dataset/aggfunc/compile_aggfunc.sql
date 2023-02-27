{%- macro compile_aggfunc(column_name, aggfunc, activity_name, table_alias, attribute_data_type) -%}
    {{ adapter.dispatch('compile_aggfunc', 'dbt_activity_schema')(column_name, aggfunc, activity_name, table_alias, attribute_data_type) }}
{%- endmacro -%}


{%- macro default__compile_aggfunc(column_name, aggfunc, activity_name, table_alias, attribute_data_type) -%}
{%- set alias_col = table_alias~'.'~column_name -%}
{%- if aggfunc == 'first_value' -%}
{{ dbt_activity_schema.aggfunc_first_value(alias_col, activity_name, table_alias, attribute_data_type) }}
{%- elif aggfunc == 'last_value' -%}
{{ dbt_activity_schema.aggfunc_last_value(alias_col, activity_name, table_alias, attribute_data_type) }}
{%- elif aggfunc == 'notnull' -%}
{{ dbt_activity_schema.aggfunc_notnull(alias_col) }}
{%- elif aggfunc == 'listagg' -%}
{{ dbt_activity_schema.aggfunc_listagg(alias_col) }}
{%- elif aggfunc == 'sum_bool' -%}
{{ dbt_activity_schema.aggfunc_sum_bool(alias_col) }}
{%- elif aggfunc == 'count_distinct' -%}
{{ dbt_activity_schema.aggfunc_sum_bool(alias_col) }}
{%- else -%}
{{aggfunc}}({{alias_col}})
{%- endif -%}

{%- endmacro -%}
