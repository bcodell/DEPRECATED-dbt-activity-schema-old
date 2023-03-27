{% macro type_json() %}
    {{ return(adapter.dispatch('type_json', 'dbt_activity_schema')()) }}
{% endmacro %}

{% macro default__type_json() %}
object
{% endmacro %}

{% macro snowflake__type_json() %}
object
{% endmacro %}

{% macro redshift__type_json() %}
super
{% endmacro %}

{% macro postgres__type_json() %}
jsonb
{% endmacro %}

{% macro bigquery__type_json() %}
json
{% endmacro %}

{% macro get_max_ts(activity_stream_relation, entity_id, activity_name) -%}
    {{ return(adapter.dispatch('get_max_ts', 'dbt_activity_schema')(activity_stream_relation, entity_id, activity_name)) }}
{% endmacro %}

{% macro default__get_max_ts(activity_stream_relation, entity_id, activity_name) -%}
{%- set clean_activity_name = dbt_activity_schema.remove_prefix(activity_name) -%}
{%- set max_ts_query -%}
select coalesce(max(activity_at), cast('100-01-01' as {{type_timestamp()}})) as max_ts
from {{ activity_stream_relation }}
where activity_name = '{{clean_activity_name}}'
{%- endset -%}

{%- set result = run_query(max_ts_query) -%}
{%- if execute -%}
{%- set max_ts = result.columns[0].values()[0] -%}
{%- else -%}
{%- set max_ts = none -%}
{%- endif -%}

{{ return(max_ts) }}

{% endmacro %}

{% macro get_cluster_statement(entity_id) %}
    {{ return(adapter.dispatch('get_cluster_statement', 'dbt_activity_schema')(entity_id)) }}
{% endmacro %}

{% macro default__get_cluster_statement(entity_id) %}
cluster by activity_name, {{entity_id}}
{% endmacro %}

{% macro snowflake__get_cluster_statement(entity_id) %}
cluster by (activity_name, activity_occurrence in (1, NULL), activity_repeated_at is NULL, to_date(activity_at))
{% endmacro %}

{% macro bigquery__get_cluster_statement(entity_id) %}
cluster by activity_name, {{entity_id}}
{% endmacro %}

{% macro redshift__get_cluster_statement(entity_id) %}
partition by activity_name, {{entity_id}}
{% endmacro %}

{% macro postgres__get_cluster_statement(entity_id) %}
partition by activity_name, {{entity_id}}
{% endmacro %}


{% macro resume_cluster(relation) %}
    {{ return(adapter.dispatch('resume_cluster', 'dbt_activity_schema')(relation)) }}
{% endmacro %}

{% macro default__resume_cluster(relation) %}
alter table {{relation}} resume recluster
{% endmacro %}

{% macro snowflake__resume_cluster(relation) %}
alter table {{relation}} resume recluster
{% endmacro %}

{% macro bigquery__resume_cluster(relation) %}
-- noop - recluster happens automatically
{% endmacro %}

{% macro redshift__resume_cluster(relation) %}
vacuum {{relation}}
{% endmacro %}

{% macro postgres__resume_cluster(relation) %}
vacuum {{relation}}
{% endmacro %}


{% macro get_activity_ts_col() %}
    {{ return(adapter.dispatch('get_activity_ts_col', 'dbt_activity_schema')()) }}
{% endmacro %}

{% macro default__get_activity_ts_col() %}
    {{ return('activity_at') }}
{% endmacro %}


{% macro get_activity_id_col() %}
    {{ return(adapter.dispatch('get_activity_id_col', 'dbt_activity_schema')()) }}
{% endmacro %}

{% macro default__get_activity_id_col() %}
    {{ return('activity_id') }}
{% endmacro %}


{% macro get_activity_name_col() %}
    {{ return(adapter.dispatch('get_activity_name_col', 'dbt_activity_schema')()) }}
{% endmacro %}

{% macro default__get_activity_name_col() %}
    {{ return('activity_name') }}
{% endmacro %}


{% macro get_attributes_col() %}
    {{ return(adapter.dispatch('get_attributes_col', 'dbt_activity_schema')()) }}
{% endmacro %}

{% macro default__get_attributes_col() %}
    {{ return('attributes') }}
{% endmacro %}


{% macro get_loaded_at_col() %}
    {{ return(adapter.dispatch('get_loaded_at_col', 'dbt_activity_schema')()) }}
{% endmacro %}

{% macro default__get_loaded_at_col() %}
    {{ return('___activity_stream_loaded_at') }}
{% endmacro %}


{% macro get_activity_occurrence_col() %}
    {{ return(adapter.dispatch('get_activity_occurrence_col', 'dbt_activity_schema')()) }}
{% endmacro %}

{% macro default__get_activity_occurrence_col() %}
    {{ return('activity_occurrence') }}
{% endmacro %}


{% macro get_activity_repeated_at_col() %}
    {{ return(adapter.dispatch('get_activity_repeated_at_col', 'dbt_activity_schema')()) }}
{% endmacro %}

{% macro default__get_activity_repeated_at_col() %}
    {{ return('activity_repeated_at') }}
{% endmacro %}


{% macro get_activity_stream_schema(entity_id) %}
    {{ return(adapter.dispatch('get_activity_stream_schema', 'dbt_activity_schema')(entity_id)) }}
{% endmacro %}

{% macro default__get_activity_stream_schema(entity_id) %}
{%- set activity_ts_col = dbt_activity_schema.get_activity_ts_col() -%}
{%- set activity_id_col = dbt_activity_schema.get_activity_id_col() -%}
{%- set activity_name_col = dbt_activity_schema.get_activity_name_col() -%}
{%- set activity_occurrence_col = dbt_activity_schema.get_activity_occurrence_col() -%}
{%- set activity_repeated_at_col = dbt_activity_schema.get_activity_repeated_at_col() -%}
{%- set attribues_col = dbt_activity_schema.get_attributes_col() -%}
{%- set loaded_at_col = dbt_activity_schema.get_loaded_at_col() -%}
{%- set base_columns = {
    entity_id: {'data_type': type_string(), 'sql': entity_id},
    activity_id_col: {'data_type': type_string(), 'sql': activity_id_col},
    activity_name_col: {'data_type': type_string(), 'sql': activity_name_col},
    activity_ts_col: {'data_type': type_timestamp(), 'sql': activity_ts_col},
    attribues_col: {'data_type': dbt_activity_schema.type_json(), 'sql': attribues_col},
    activity_occurrence_col: {'data_type': type_int(), 'sql': activity_occurrence_col},
    activity_repeated_at_col: {'data_type': type_timestamp(), 'sql': activity_repeated_at_col},
    loaded_at_col: {'data_type': type_timestamp(), 'sql': current_timestamp()}
} -%}
    {{ return(base_columns) }}
{% endmacro %}


{% macro generate_select_activity_sql(entity_id, activity_name=none, max_ts=none, create_mode=false) -%}
    {{ return(adapter.dispatch('generate_select_activity_sql', 'dbt_activity_schema')(entity_id, activity_name, max_ts, create_mode)) }}
{% endmacro %}

{% macro default__generate_select_activity_sql(entity_id, activity_name=none, max_ts=none, create_mode=false) -%}
{%- set base_columns = dbt_activity_schema.get_activity_stream_schema(entity_id=entity_id) -%}

{%- set select_query -%}
select
{% for key in base_columns.keys() %}
    {% if not loop.first %}, {% endif %}cast({% if activity_name is not none %}{{base_columns[key]['sql']}}{% else %} null{% endif %} as {{base_columns[key]['data_type']}}) as {{key}}
{% endfor %}
{% if activity_name is not none %}
from {{ ref(activity_name) }}
{% endif %}
{% if max_ts is not none %}
{%- set activity_ts_col = dbt_activity_schema.get_activity_ts_col() -%}
where {{activity_ts_col}} > cast('{{max_ts}}' as {{type_timestamp()}})
{% endif %}
{% if create_mode %}
{{dbt_activity_schema.get_cluster_statement(entity_id=entity_id)}}
{% endif %}
{%- endset -%}

{{ return(select_query) }}

{% endmacro %}

{% macro get_cluster_keys(entity_id) %}
    {{ return(adapter.dispatch('get_cluster_keys', 'dbt_activity_schema')(entity_id)) }}
{% endmacro %}

{% macro default__get_cluster_keys(entity_id) %}
{%- set activity_name_col = dbt_activity_schema.get_activity_name_col() -%}
cluster by ({{activity_name_col}}, {{entity_id}})
{% endmacro %}

{% macro snowflake__get_cluster_keys(entity_id) %}
{%- set activity_name_col = dbt_activity_schema.get_activity_name_col() -%}
cluster by (activity_name, activity_occurrence in (1, NULL), activity_repeated_at is NULL, to_date(activity_at))
{% endmacro %}

{% macro bigquery__get_cluster_keys(entity_id) %}
{%- set activity_name_col = dbt_activity_schema.get_activity_name_col() -%}
cluster by ({{activity_name_col}}, {{entity_id}})
{% endmacro %}

{% macro redshift__get_cluster_keys(entity_id) %}
{%- set activity_name_col = dbt_activity_schema.get_activity_name_col() -%}
{%- set activity_ts_col = dbt_activity_schema.get_activity_ts_col() -%}
distkey(entity_id)
compound sortkey({{entity_id}}, {{activity_name_col}}, {{activity_ts_col}})
{% endmacro %}

{% macro postgres__get_cluster_keys(entity_id) %}
-- noop - not supporting for now
{% endmacro %}

{% macro transient() %}
    {{ return(adapter.dispatch('transient', 'dbt_activity_schema')()) }}
{% endmacro %}

{% macro default__transient() %}

{% endmacro %}

{% macro snowflake__transient() %}
transient
{% endmacro %}

{% macro create_empty_activity_stream(entity_id, relation) -%}
    {{ return(adapter.dispatch('create_empty_activity_stream', 'dbt_activity_schema')(entity_id, relation)) }}
{% endmacro %}

{% macro default__create_empty_activity_stream(entity_id, relation) %}
{%- set base_columns = dbt_activity_schema.get_activity_stream_schema(entity_id=entity_id) -%}

create {{dbt_activity_schema.transient()}} table {{relation.include(database=true)}} (
{% for key in base_columns.keys() %}
    {% if not loop.first %}, {% endif %}{{key}} {{base_columns[key]['data_type']}}
{% endfor %}
)
{{ dbt_activity_schema.get_cluster_keys(entity_id=entity_id) }}
{% endmacro %}
