{% macro build_dataset(activity_stream, primary_activity, primary_activity_attributes, aggregations) %}
{{ adapter.dispatch('build_dataset', 'dbt_activity_schema')(activity_stream, primary_activity, primary_activity_attributes, aggregations) }}
{% endmacro %}

{% macro default__build_dataset(activity_stream, primary_activity, primary_activity_attributes, aggregations) %}
{%- set entity_id = var('dbt_activity_schema')[activity_stream]['entity_id'] -%}
{% set standard_columns = [
    'activity_id',
    entity_id,
    'activity_at',
    'activity_occurrence',
    'activity_repeated_at'
]
%}
{%- set project_name = model['unique_id'].split('.')[1] -%}

{%- if execute -%}
{# parameter checks #}

    {# confirm primary activity exists #}
    {%- set primary_activity_key = 'model.' ~ project_name ~ '.' ~ primary_activity -%}
    {%- set primary_activity_node = graph.nodes.get(primary_activity_key, none) -%}
    {%- if primary_activity_node is none -%}
        {%- set error_message -%}
        Model '{{ model.unique_id }}' references invalid model '{{ primary_activity }}' as the primary_activity argument in the build_dataset macro. Please specify a valid model for this argument.
        {%- endset -%}
        {{ exceptions.raise_compiler_error(error_message) }}
    {%- endif -%}

    {# confirm primary event attributes exist #}
    {%- if primary_activity_attributes is not none -%}
        {%- set primary_activity_key = 'model.' ~ project_name ~ '.' ~ primary_activity -%}
        {%- set primary_activity_node = graph.nodes.get(primary_activity_key) -%}
        {%- for attr_name in primary_activity_attributes -%}
            {%- set attr_dict = primary_activity_node.config['attributes'].get(attr_name, none) -%}
            {%- if attr_dict is none -%}
                {%- set attr_list = primary_activity_node.config['attributes'].keys()|list -%}
                {%- set attr_str = attr_list|join(', ') -%}
                {%- set error_message -%}
                Model '{{ model.unique_id }}' references invalid attribute '{{ attr_name }}' from primary activity '{{ primary_activity }}' in the primary_activity_attributes argument of the build_dataset macro. Please specify a valid attribute. Valid attributes are '{{ attr_str }}'
                {%- endset -%}
                {{ exceptions.raise_compiler_error(error_message) }}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}

    {# confirm aggregations exist #}
    {%- if aggregations is not none -%}
        {%- for aggregation in aggregations -%}
            {%- set agg_key = 'model.' ~ project_name ~ '.' ~ aggregation -%}
            {%- set agg_node = graph.nodes.get(agg_key, none) -%}
            {%- if agg_node is none -%}
                {%- set error_message -%}
                Model '{{ model.unique_id }}' references invalid aggregation model '{{ agg_key }}' in the aggregations argument of the build_dataset macro. Please specify a valid aggregation for this dataset.
                {%- endset -%}
                {{ exceptions.raise_compiler_error(error_message) }}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}
{%- endif -%}

{%- set sql_graph = {
    'primary_activity': primary_activity,
    'primary_activity_cte': 'primary__'~primary_activity,
    'primary_activity_attributes': [],
    'join_requirements': [],
    'secondary_activities': {}
} -%}


{% if execute %}
{%- if primary_activity_attributes is not none -%}
{%- set primary_activity_key = 'model.' ~ project_name ~ '.' ~ primary_activity -%}
{%- set primary_activity_node = graph.nodes.get(primary_activity_key) -%}
{%- for pe in primary_activity_attributes -%}
{%- do sql_graph['primary_activity_attributes'].append(
    {
        'attribute_name': pe,
        'parsed_attribute': dbt_activity_schema.parse_activity_attribute(primary_activity_node, pe),
        'column_name': dbt_activity_schema.remove_prefix(primary_activity~'_'~pe)
    }
)
-%}
{%- set cn = dbt_activity_schema.remove_prefix(primary_activity~'_'~pe) -%}
{{ log('pa attribute name1 '~cn)}}
{{ log('pa attribute name2 '~sql_graph['primary_activity_attributes'][-1]['column_name'])}}
{%- endfor -%}
{%- endif -%}

{% for aggregation in aggregations %}
    {{ log('full agg'~aggregation)}}
    {{ log('agg include'~aggregation)}}
    {% for node in graph.nodes.values() | selectattr("name", "equalto", aggregation) %}
        {%- if node.name == aggregation -%}
            {%- set secondary_activity = node.config.activity_name -%}
            {%- set secondary_activity_cte = 'secondary__'~secondary_activity -%}
            {%- if secondary_activity not in sql_graph['secondary_activities'].keys() -%}
                {%- do sql_graph['secondary_activities'].update({
                    secondary_activity: {
                        'activity_name': secondary_activity,
                        'cte': secondary_activity_cte,
                        'joins': {}
                    }
                }) -%}
            {%- endif -%}

            {%- set after_ts = node.config.get('after_timestamp', none) -%}
            {%- set before_ts = node.config.get('before_timestamp', none) -%}
            {%- set relationship = node.config.get('relationship') -%}
            {%- do sql_graph['join_requirements'].append(after_ts) -%}
            {%- do sql_graph['join_requirements'].append(before_ts) -%}
            {%- set join_key = (relationship, after_ts, before_ts) -%}
            {%- set table_alias = secondary_activity~'_'~relationship~'_'~dbt_activity_schema.remove_punctuation(after_ts)~'_'~dbt_activity_schema.remove_punctuation(before_ts) -%}
            {%- if join_key not in sql_graph['secondary_activities'][secondary_activity]['joins'].keys() -%}
                {%- do sql_graph['secondary_activities'][secondary_activity]['joins'].update({
                    join_key: {
                        'relationship': relationship,
                        'after_ts': after_ts,
                        'before_ts': before_ts,
                        'table_alias': table_alias,
                        'aggregations': []
                    }
                }) -%}
            {%- endif -%}
            {%- set aggregation_name = dbt_activity_schema.remove_prefix(node.name) -%}
            {%- set aggfunc = node.config.aggfunc -%}
            {%- set attribute_name = node.config.attribute -%}
            {%- set secondary_activity_key = 'model.' ~ project_name ~ '.' ~ secondary_activity -%}
            {{ log('secondary activity key '~secondary_activity_key) }}
            {%- set secondary_activity_node = graph.nodes.get(secondary_activity_key) -%}
            {{ log('secondary activity node '~secondary_activity_node) }}
            {%- if attribute_name in ['activity_id', 'activity_name', 'activity_at', 'activity_repeated_at', 'activity_occurrence'] -%}
                {%- set attribute_data_type = {
                    'activity_id': type_string(),
                    'activity_name': type_string(),
                    'activity_at': 'timestamp',
                    'activity_repeated_at': 'timestamp',
                    'activity_occurrence': type_int(),
                }[attribute_name]
                -%}
            {%- else -%}
                {%- set attribute_data_type = secondary_activity_node.config['attributes'][attribute_name].get('data_type', none) -%}
            {%- endif -%}
            {{ log(attribute_name~' '~attribute_data_type)}}
            {%- set parsed_attribute = dbt_activity_schema.parse_aggregation_attribute(node, attribute_data_type) -%}
            {%- set aggregation_sql = dbt_activity_schema.compile_aggfunc(
                column_name=aggregation_name,
                aggfunc=aggfunc,
                activity_name=secondary_activity,
                table_alias=table_alias,
                attribute_data_type=attribute_data_type
            ) -%}
            {%- do sql_graph['secondary_activities'][secondary_activity]['joins'][join_key]['aggregations'].append({
                'aggregation_name': aggregation_name,
                'aggfunc': aggfunc,
                'attribute_name': attribute_name,
                'parsed_attribute': parsed_attribute,
                'aggregation_sql': aggregation_sql
            }) -%}
        {%- endif -%}
    {% endfor -%}
{% endfor -%}
{% endif %}

{%- set primary_columns = [] %}
{%- set cleaned_primary_activity = dbt_activity_schema.remove_prefix(primary_activity) -%}


{%- for aggregation in aggregations %}
{%- set aggregation_dependency = "-- depends_on: "~ref(aggregation) -%}
{{aggregation_dependency}}
{%- endfor %}

{%- set primary_cte = sql_graph['primary_activity_cte'] %}

with {{primary_cte}} as (
    select
        {%- for sc in standard_columns -%}
        {% set primary_sc = dbt_activity_schema.remove_prefix(sql_graph['primary_activity'])~'_'~sc %}
        {% if not loop.first %}, {% endif %}{{sc}} as {{primary_sc}}
        {%- do primary_columns.append(primary_sc) -%}
        {% endfor %}
        {%- for attr in sql_graph['primary_activity_attributes'] %}
        , {{attr['parsed_attribute']}} as {{attr['column_name']}}
        {%- do primary_columns.append(attr['column_name']) -%}
        {%- endfor %}
    from {{ ref(activity_stream) }}
    where activity_name = '{{cleaned_primary_activity}}'
)
{% for secondary_activity in sql_graph['secondary_activities'].keys() -%}
{%- set cleaned_secondary_activity = dbt_activity_schema.remove_prefix(secondary_activity) -%}
{%- set se = sql_graph['secondary_activities'][secondary_activity] -%}
, {{se['cte']}} as (
    select
        {% for sc in standard_columns -%}
        {%- if not loop.first -%}, {% endif -%}{{sc}} as {{secondary_activity}}_{{sc}}
        {% endfor -%}
        {%- for j in se['joins'].keys() -%}
        {%- set join_reqs = se['joins'][j] -%}
        {% for sm in join_reqs['aggregations'] -%}
        , {{sm['parsed_attribute']}} as {{sm['aggregation_name']}}
        {%- endfor -%}
        {% endfor %}
    from {{ ref(activity_stream) }}
    where activity_name = '{{cleaned_secondary_activity}}'
)
{% endfor -%}

{%- for secondary_activity in sql_graph['secondary_activities'].keys() -%}
{%- set se = sql_graph['secondary_activities'][secondary_activity] -%}
{%- for j in se['joins'].keys() -%}
{%- set join_reqs = se['joins'][j] -%}
{%- set alias = join_reqs['table_alias'] %}
, {{alias}} as (
    select
    {% if join_reqs['relationship'] != 'aggregate_all_ever' %}
        {{primary_cte}}.{{dbt_activity_schema.remove_prefix(sql_graph['primary_activity'])}}_{{entity_id}}
        , {{primary_cte}}.{{dbt_activity_schema.remove_prefix(sql_graph['primary_activity'])}}_activity_id
    {% else %}
        {{alias}}.{{dbt_activity_schema.remove_prefix(sql_graph['primary_activity'])}}_{{entity_id}}
    {% endif %}
        {%- for sm in join_reqs['aggregations'] %}
        , {{sm['aggregation_sql']}} as {{sm['aggregation_name']}}
        {%- endfor %}
    {% if join_reqs['relationship'] != 'aggregate_all_ever' %}
    from {{primary_cte}}
    left join {{se['cte']}} {{alias}}
        on {{primary_cte}}.{{dbt_activity_schema.remove_prefix(sql_graph['primary_activity'])}}_{{entity_id}} = {{alias}}.{{secondary_activity}}_{{entity_id}}
        on {{primary_cte}}.{{dbt_activity_schema.remove_prefix(sql_graph['primary_activity'])}}_{{entity_id}} = {{alias}}.{{secondary_activity}}_{{entity_id}}
        {{ dbt_activity_schema.compile_relationship_join(
            primary_activity=dbt_activity_schema.remove_prefix(sql_graph['primary_activity']),
            primary_cte=primary_cte,
            secondary_activity=secondary_activity,
            secondary_alias=alias,
            relationship=join_reqs['relationship'],
            nth_occurrence=none,
            after_timestamp=join_reqs['after_ts'],
            before_timestamp=join_reqs['before_ts']
        ) }}
    {% else %}
    from {{se['cte']}} {{alias}}
    group by
        {% if join_reqs['relationship'] != 'aggregate_all_ever' %}
            {{primary_cte}}.{{dbt_activity_schema.remove_prefix(sql_graph['primary_activity'])}}_{{entity_id}}
            , {{primary_cte}}.{{dbt_activity_schema.remove_prefix(sql_graph['primary_activity'])}}_activity_id
        {% else %}
            {{alias}}.{{dbt_activity_schema.remove_prefix(sql_graph['primary_activity'])}}_{{entity_id}}
        {% endif %}
)
{% endfor -%}
{%- endfor -%}
, joined_full as (
    select
        {%- for pc in primary_columns %}
        {% if not loop.first -%}, {% endif -%}{{primary_cte}}.{{pc}}
        {%- endfor %}
        {%- for secondary_activity in sql_graph['secondary_activities'].keys() -%}
        {%- set se = sql_graph['secondary_activities'][secondary_activity] -%}
        {%- for j in se['joins'].keys() -%}
        {%- set join_reqs = se['joins'][j] -%}
        {%- for sm in join_reqs['aggregations'] %}
        , {{join_reqs['table_alias']}}.{{sm['aggregation_name']}}
        {%- endfor -%}
        {%- endfor -%}
        {% endfor %}
    from {{primary_cte}}
    {% for secondary_activity in sql_graph['secondary_activities'].keys() -%}
    {%- set se = sql_graph['secondary_activities'][secondary_activity] -%}
    {% for j in se['joins'].keys() %}
    {%- set join_reqs = se['joins'][j] -%}
    {%- set alias = join_reqs['table_alias'] -%}
    left join {{alias}}
    {% if join_reqs['relationship'] != 'aggregate_all_ever' %}
        on {{primary_cte}}.{{dbt_activity_schema.remove_prefix(sql_graph['primary_activity'])}}_activity_id = {{alias}}.{{dbt_activity_schema.remove_prefix(sql_graph['primary_activity'])}}_activity_id
    {% else %}
        on {{primary_cte}}.{{dbt_activity_schema.remove_prefix(sql_graph['primary_activity'])}}_{{entity_id}} = {{alias}}.{{dbt_activity_schema.remove_prefix(sql_graph['primary_activity'])}}_{{entity_id}}
    {% endfor %}
    {%- endfor -%}

)
select *
from joined_full

{% endmacro %}