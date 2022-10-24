{% materialization aggregation, default %}
  {%- set target_relation = api.Relation.create(identifier=model['alias'], type='cte') -%}
  {% set macro_deps = model['depends_on']['macros'] -%}
  {% if 'macro.dbt_activity_schema.build_aggregation' not in macro_deps %}
    {%- set error_message -%}
      Aggregation model '{{ model.unique_id }}' is missing required dependency on the build_aggregation macro. The macro should be explicitly called in the model.
    {%- endset -%}
    {{ exceptions.raise_compiler_error(error_message) }}
  {% endif %}
      

  {% call noop_statement('main', model.unique_id) -%}
    {{sql}}
  {%- endcall %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}