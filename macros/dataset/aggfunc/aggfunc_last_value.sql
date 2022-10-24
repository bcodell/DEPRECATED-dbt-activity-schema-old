{%- macro aggfunc_last_value(column, activity_name, table_alias, attribute_data_type) -%}
    {{ adapter.dispatch('aggfunc_last_value', 'dbt_activity_schema')(column, activity_name, table_alias, attribute_data_type) }}
{%- endmacro -%}

{%- macro default__aggfunc_last_value(column, activity_name, table_alias, attribute_data_type) -%}
{%- set activity_col = activity_name~'_activity_at' -%}
{%- set delimiter = ';.,;' -%}
cast(split_part(
            max(
                cast({{table_alias}}.{{activity_col}} as varchar)
                || '{{delimiter}}'
                || cast({{column}} as varchar)
            ),
            '{{delimiter}}',
            2
        ) as {{attribute_data_type}})
{%- endmacro -%}
