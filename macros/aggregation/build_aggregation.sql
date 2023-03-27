{% macro build_aggregation(activity_stream) %}
{{ adapter.dispatch('build_aggregation', 'dbt_activity_schema')(activity_stream)}}
{% endmacro %}

{% macro default__build_aggregation(activity_stream) %}
{%- set activity_name = config.require('activity_name') -%}
{%- set attribute = config.require('attribute') -%}
{%- set aggfunc = config.require('aggfunc') -%}
{%- set after_timestamp = config.require('after_timestamp') -%}
{%- set before_timestamp = config.require('before_timestamp') -%}
{%- set relationship = config.require('relationship') -%}
{%- set condition = config.get('condition', default=none) -%}
{%- set backup_value = config.get('backup_value', default=none) -%}
{%- set relationships = dbt_activity_schema._get_relationships() -%}

  {%- if execute -%}
    {# confirm aggregation materialization #}
    {%- set materialization = config.get('materialized') -%}
    {%- if materialization != 'aggregation' -%}
      {%- set error_message -%}
        Model '{{ model.unique_id }}' is an activity stream aggregation and needs to have an aggregation materialization type. The current materialization type is '{{materialization}}'
      {%- endset -%}
      {{ exceptions.raise_compiler_error(error_message) }}
    {%- endif -%}

    {# confirm dependency #}
    {%- set model_node = graph.nodes.get(model['unique_id']) -%}
    {%- set model_deps = model_node['depends_on']['nodes'] -%}
    {%- set dependency_check = {'activity_stream_dependencies': 0} -%}
    {%- for dep in model_deps -%}
      {%- set dep_node = graph.nodes.get(dep) -%}
      {{ log('dep node '~dep_node~dep_node.config.materialized) }}
      {{ log('side by side '~dep_node.config.materialized~' activity_stream')}}
      {{ log('bool check '~dep_node.config.materialized == 'activity_stream') }}
      {%- if dep_node.config.materialized == 'activity_stream' -%}
      {%- do dependency_check.update({'activity_stream_dependencies': 1}) -%}
      {%- endif -%}
    {%- endfor -%}
    {{ log('activity stream dependency '~has_activity_stream_dependency) }}
    {%- if dependency_check['activity_stream_dependencies'] == 0 -%}
      {%- set error_message -%}
        Model '{{ model.unique_id }}' does not have a dependency on any activity stream model.
      {%- endset -%}
      {{ exceptions.raise_compiler_error(error_message) }}
    {%- endif -%}

    {# confirm valid relationship is specified #}
    {%- if relationship not in relationships.keys() -%}
        {%- set error_message -%}
        Relationship '{{ relationship }}' is invalid for model '{{ model.unique_id }}'. Please specify a valid option from {{ relationships.keys() | join(', ') }}
        {%- endset -%}
        {{ exceptions.raise_compiler_error(error_message) }}
    {%- endif -%}
  {%- endif -%}



{{"-- depends_on: "~activity_stream}}

{% endmacro %}