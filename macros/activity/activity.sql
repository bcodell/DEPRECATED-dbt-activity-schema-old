{% materialization activity, default %}
  {%- set materialization_strategy = config.get('materialization_strategy', 'table') -%}
  {# this is entirely a copy-pasta of the table materialization from dbt-core 1.2.0 #}

  {%- set valid_materialization_strategies = ['table'] -%}
  {%- if materialization_strategy not in valid_materialization_strategies -%}
      {%- set error_message -%}
          Model '{{ model.unique_id }}' has an invalid materialization strategy of '{{ materialization_strategy }}'.
          Valid strategies are '{{ valid_materialization_strategies|join(", ") }}'.
      {%- endset -%}
      {{ exceptions.raise_compiler_error(error_message) }}
  {%- endif -%}

  {% set macro_deps = model['depends_on']['macros'] -%}
  {% if 'macro.dbt_activity_schema.build_activity' not in macro_deps %}
    {%- set error_message -%}
      Activity model '{{ model.unique_id }}' is missing required dependency on the build_activity macro. The macro should be explicitly called in the model.
    {%- endset -%}
    {{ exceptions.raise_compiler_error(error_message) }}
  {% endif %}


  -- run code for table model

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') %}
  {%- set intermediate_relation =  make_intermediate_relation(target_relation) -%}
  -- the intermediate_relation should not already exist in the database; get_relation
  -- will return None in that case. Otherwise, we get a relation that we can drop
  -- later, before we try to use this name for the current operation
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
  /*
      See ../view/view.sql for more information about this relation.
  */
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
  -- as above, the backup_relation should not already exist
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
  -- grab current tables grants config for comparision later on
  {% set grant_config = config.get('grants') %}

  -- drop the temp relations if they exist already in the database
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% call statement('main') -%}
    {{ get_create_table_as_sql(False, intermediate_relation, sql) }}
  {%- endcall %}

  -- cleanup
  {% if existing_relation is not none %}
      {{ adapter.rename_relation(existing_relation, backup_relation) }}
  {% endif %}

  {{ adapter.rename_relation(intermediate_relation, target_relation) }}

  {% do create_indexes(target_relation) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  -- finally, drop the existing/backup relation after the commit
  {{ drop_relation_if_exists(backup_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}


{% endmaterialization %}
