{%- macro parse_json(json_col, key) -%}
    {{ adapter.dispatch('parse_json', 'dbt_activity_schema')(json_col, key) }}
{%- endmacro -%}

{%- macro default__parse_json(json_col, key) -%}
json_extract_path_text({{json_col}}::json, '{{key}}')
{%- endmacro -%}

{%- macro snowflake__parse_json(json_col, key) -%}
to_varchar(get_path({{json_col}}, '{{key}}'))
{%- endmacro -%}

{%- macro parse_activity_attribute(activity_node, attribute) -%}
    {{ adapter.dispatch('parse_activity_attribute', 'dbt_activity_schema')(activity_node, attribute) }}
{%- endmacro -%}

{%- macro default__parse_activity_attribute(activity_node, attribute) -%}
{%- set data_type = activity_node.config['attributes'][attribute]['data_type'] -%}
cast(nullif({{dbt_activity_schema.parse_json('attributes', attribute)}}, '') as {{data_type}})
{%- endmacro -%}


{%- macro parse_aggregation_attribute(aggregation_node, data_type) -%}
    {{ adapter.dispatch('parse_aggregation_attribute', 'dbt_activity_schema')(aggregation_node, data_type) }}
{%- endmacro -%}

{%- macro default__parse_aggregation_attribute(aggregation_node, data_type) -%}
{%- set aggregation_node_deps = aggregation_node.get('depends_on')['nodes'] -%}
{%- set activity_stream_dict = {'name': ''} -%}
{%- for dep in aggregation_node_deps -%}
    {%- if graph.nodes.get(dep)['config']['materialized'] == 'activity_stream' -%}
        {%- do activity_stream_dict.update({'name': dep.split('.')[-1]}) -%}
    {%- endif -%}
{%- endfor -%}
{%- set entity_id = var('entity_id', var('dbt_activity_schema')[activity_stream_dict['name']]['entity_id']) -%}
{%- set attribute_name = aggregation_node.config.get('attribute') -%}
{%- set condition = aggregation_node.config.get('condition', none) -%}
{%- if condition is none -%}
    {%- set condition_str = '' -%}
{%- else -%}
    {%- set condition_str = ' '~condition -%}
{%- endif -%}
{%- set backup_value = aggregation_node.config.get('backup_value', none) -%}
{%- if backup_value is none -%}
    {%- set coalesce_prefix = '' -%}
    {%- set coalesce_suffix = '' -%}
{%- else -%}
    {%- set coalesce_prefix = 'coalesce(' -%}
    {%- set coalesce_suffix = ', '~backup_value~')' -%}
{%- endif -%}
{%- if attribute_name in ['activity_id', 'activity_at', entity_id] -%}
{{coalesce_prefix}}{{attribute_name}}{{condition_str}}{{coalesce_suffix}}
{%- else -%}
{{coalesce_prefix}}cast(nullif({{dbt_activity_schema.parse_json('attributes', attribute_name)}}, '')as {{data_type}}){{condition_str}}{{coalesce_suffix}}
{%- endif -%}
{%- endmacro -%}
