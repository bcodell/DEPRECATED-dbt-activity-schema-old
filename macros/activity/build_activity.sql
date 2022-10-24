{#
build_activity: Compiles a final select statement in a standardized format so that all activity models can be unioned together in the final activity stream model. For each activity transformation model, a final CTE should be compiled containing the primary customer id used in the activity stream, a timestamp column representing when the timestamp occurred, and all of the static attributes associated with the activity, then this macro should be applied after that CTE. No aggregations or transformations beyond basic selecting and column aliasing will occur in this macro.
    - cte: string; the name of the last CTE in the query containing all relevant columns to compile the activity model
#}

{% macro build_activity(cte) %}
{{ adapter.dispatch('build_activity', 'dbt_activity_schema')(cte) }}
{% endmacro %}


{% macro default__build_activity(cte) %}
  {%- set model_name = dbt_activity_schema.remove_prefix(model.name) -%}
  {%- if execute -%}
    {%- set activity_stream = config.require('activity_stream') -%}
    {%- set entity_id = var('dbt_activity_schema')[activity_stream]['entity_id'] -%}
    {{log('build activity '~model.name~' '~activity_stream~' '~entity_id)}}


    {# make sure attributes are defined appropriately #}
    {%- set attributes = config.get('attributes') or none -%}
    {%- if attributes is not none -%}
      {%- set required_keys = ['data_type'] -%}
      {%- set description_required = var('dbt_activity_schema')[activity_stream].get('require_attribute_descriptions', false) -%}
      {%- if description_required -%}
        {%- do required_keys.append('description') -%}
      {%- endif -%}
      {%- set attribute_aliases = attributes.keys() -%}
      {%- for a in attribute_aliases -%}
        {%- for rk in required_keys -%}
          {%- if rk not in attributes[a].keys() -%}
            {%- set error_message -%}
              Model '{{ model.unique_id }}' is missing required attribute element '{{rk}}' for attribute '{{a}}'.
            {%- endset -%}
            {{ exceptions.raise_compiler_error(error_message) }}
          {%- endif -%}
        {%- endfor -%}
      {%- endfor -%}
    {%- endif -%}

    {# confirm that model is defined as a dependency on the activity stream #}
    {%- set project_name = model['unique_id'].split('.')[1] -%}
    {%- set activity_stream_key = 'model.' ~ project_name ~ '.' ~ activity_stream -%}
    {%- set activity_stream_node = graph.nodes.get(activity_stream_key) -%}
    {%- set activity_stream_deps = activity_stream_node['depends_on']['nodes'] -%}
    {%- if model['unique_id'] not in activity_stream_deps -%}
      {%- set error_message -%}
        Model '{{ model.unique_id }}' is not properly assigned as a dependency for activity stream '{{activity_stream}}'.
      {%- endset -%}
      {{ exceptions.raise_compiler_error(error_message) }}
    {%- endif -%}


  {% else %}
  {%- set activity_stream = 'none' -%}
  {%- set attributes = none -%}
  {%- set entity_id = 'none' -%}
  {{log(model.name~' first parse')}}

  {% endif %}
{%- set activity_at_column = dbt_activity_schema.get_activity_ts_col() -%}

select
    cast({{ dbt_activity_schema.surrogate_key([entity_id, activity_at_column, "'"~model.name~"'"]) }} as {{type_string()}}) as activity_id
    , cast({{ entity_id }} as {{type_string()}}) as {{ entity_id }}
    , cast('{{model_name}}' as {{type_string()}}) as activity_name
    , cast({{activity_at_column}} as {{type_timestamp()}}) as {{activity_at_column}}
    , {{ dbt_activity_schema.attributes_to_json(attributes) }} as attributes
from {{cte}}

{% endmacro %}
