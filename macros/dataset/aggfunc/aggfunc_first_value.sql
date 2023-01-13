{%- macro aggfunc_first_value(column, activity_name, table_alias, attribute_data_type) -%}
    {{ adapter.dispatch('aggfunc_first_value', 'dbt_activity_schema')(column, activity_name, table_alias, attribute_data_type) }}
{%- endmacro -%}

{%- macro default__aggfunc_first_value(column, activity_name, table_alias, attribute_data_type) -%}
{%- set activity_col = activity_name~'_activity_at' -%}
{%- set delimiter = ';.,;' -%}
cast(nullif(split_part(
            min(
                cast({{table_alias}}.{{activity_col}} as varchar)
                || '{{delimiter}}'
                || coalesce(cast({{column}} as {{type_string()}}), '')
            ),
            '{{delimiter}}',
            2
        ), '') as {{attribute_data_type}})
{%- endmacro -%}
