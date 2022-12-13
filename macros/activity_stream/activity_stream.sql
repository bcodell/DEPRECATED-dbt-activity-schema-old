{% materialization activity_stream, default -%}
  -- validate setup
  {%- set materialization_strategy = config.get('materialization_strategy') or 'table' -%}
  {%- set valid_materialization_strategies = ['table', 'incremental'] -%}
  {%- if materialization_strategy not in valid_materialization_strategies -%}
      {%- set error_message -%}
          Model '{{ model.unique_id }}' has an invalid materialization strategy of '{{ materialization_strategy }}'.
          Valid strategies are '{{ valid_materialization_strategies|join(", ") }}'.
      {%- endset -%}
      {{ exceptions.raise_compiler_error(error_message) }}
  {%- endif -%}

  {% set macro_deps = model['depends_on']['macros'] -%}
  {% if 'macro.dbt_activity_schema.build_activity_stream' not in macro_deps %}
    {%- set error_message -%}
      Activity Stream model '{{ model.unique_id }}' is missing required dependency on the build_activity_stream macro. The macro should be explicitly called in the model.
    {%- endset -%}
    {{ exceptions.raise_compiler_error(error_message) }}
  {% endif %}

  {%- set is_incremental = materialization_strategy == 'incremental' -%}

  {%- set identity_resolution_relation = config.get('identity_resolution_model') or none -%}


  -- define primary config
  {%- set loop_vars = {'sum_rows_inserted': 0} -%}
  {%- set identifier = model['alias'] -%}
  {%- set entity_id = var('dbt_activity_schema')[identifier]['entity_id'] -%}




  -- get setup requirements
  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set relation_exists = old_relation is not none -%}
  {%- set target_relation = api.Relation.create(
    identifier=identifier,
    schema=schema,
    database=database,
    type='table'
  ) -%}
  {%- set full_refresh = flags.FULL_REFRESH or materialization_strategy == 'table' -%}
  {{ log('is_full_refresh: '~full_refresh~flags.FULL_REFRESH) }}

  -- run pre-hooks
  {{ run_hooks(pre_hooks) }}


  -- setup
  -- open with begin statement so that if failure occurs everything rolls back
  {% call statement('begin', fetch_result=True) %}
    begin;
  {%- endcall -%}

  -- create temp relation and table if table materialization strategy or full_refresh flag
  {%- if full_refresh -%}
    {%- set tmp_identifier = identifier ~ '__dbt_activity_schema' ~ '_tmp' -%}
    {%- set tmp_relation = api.Relation.create(identifier=tmp_identifier, database=database, schema=schema, type='table') -%}
    {%- set tmp_sql = dbt_activity_schema.generate_select_activity_sql(entity_id=entity_id) -%}
    {%- set backup_identifier = identifier ~ '__dbt_activity_schema' ~ '_backup' -%}
    {%- set backup_relation = api.Relation.create(identifier=backup_identifier, database=database, schema=schema, type='table') -%}
    {{ log('tmp_sql: '~tmp_sql) }}
    -- set temporary relation to false for swap later
    {{ log('creating temp table') }}
    {%- call statement('create_temp_table', fetch_result=True) -%}
      {{ dbt_activity_schema.create_empty_activity_stream(entity_id=entity_id, relation=tmp_relation) }}
    {%- endcall -%}
  {%- endif -%}
  -- create empty table if doesn't exist
  {%- if not relation_exists -%}
    {{ log('relation not exists') }}
    {%- set create_target_sql = dbt_activity_schema.generate_select_activity_sql(entity_id=entity_id) -%}
    {{ log('create_target_sql: '~create_target_sql) }}
    {%- call statement('create_empty_target_table', fetch_result=True) -%}
      {{ dbt_activity_schema.create_empty_activity_stream(entity_id=entity_id, relation=target_relation) }}
    {%- endcall -%}
    {{ log('created target ctas') }}
  {%- endif -%}
  {% set target_columns = adapter.get_columns_in_relation(target_relation) %}
  {%- set insert_target_columns = target_columns | map(attribute='quoted') | join(', ') -%}
  {{ log('target cols: '~insert_target_columns) }}

  -- get activities
  {%- set activity_list = [] -%}
  {%- for node in model['depends_on']['nodes'] -%}
    {%- do activity_list.append(node.split('.')[-1]) -%}
  {%- endfor -%}
  {%- set num_activities = activity_list|length -%}

  -- loop through each activity
  {%- for activity in activity_list -%}
    {%- set i = loop.index -%}
    {%- set activity_name = activity -%}
    {{ log('activity name '~activity_name)}}
    {%- set init_msg = "Running " ~ (i + 1) ~ " of " ~ (num_activities) ~ " for activity " ~ activity_name -%}
    {{ log(msg) }}
    {%- if not full_refresh and relation_exists -%}
      {%- set max_ts = dbt_activity_schema.get_max_ts(activity_stream_relation=target_relation, entity_id=entity_id, activity_name=activity_name) -%}
    {%- else -%}
      {%- set max_ts = none -%}
    {%- endif -%}
    {{ log('max_ts: '~max_ts) }}
    {%- set tmp_activity_identifier = activity_name ~ '_tmp' -%}
    {%- set tmp_activity_relation = api.Relation.create(identifier=tmp_activity_identifier, type='table') -%}
    {%- set insert_activity_sql = dbt_activity_schema.generate_select_activity_sql(
      entity_id=entity_id,
      activity_name=activity_name,
      max_ts=max_ts
    ) -%}
    {{ log(activity_name~' insert activity sql: '~insert_activity_sql)}}
    {%- set tmp_activity_statement = 'main_' ~ activity_name ~ '_tmp' -%}
    {%- call statement(tmp_activity_statement, fetch_result=True) -%}
      {{ get_create_table_as_sql(temporary=true, relation=tmp_activity_relation, sql=insert_activity_sql) }}
    {%- endcall -%}
    {{ log('created temp table for activity '~activity_name) }}

    -- insert data from temp activity table into activity stream
    {%- set main_activity_name = 'main-' ~ activity_name -%}
    {{ log('insert activity stream query: '~insert_activity_stream) }}
    {% call statement(main_activity_name, fetch_result=True) -%}
      insert into {% if full_refresh %}{{tmp_relation}}{% else %}{{target_relation}}{% endif %} ({{insert_target_columns}})
      (
          select
              {{insert_target_columns}}
          from {{tmp_activity_relation.include(schema=False)}}
      );
    {%- endcall %}
    {{ adapter.drop_relation(tmp_activity_relation) }}

    {% set result = load_result(main_activity_name) %}
    {{ log('result: '~result) }}
    {% if 'response' in result.keys() %} {# added in v0.19.0 #}
        {% set activity_rows_inserted = result['response']['rows_affected'] %}
    {% else %} {# older versions #}
        {% set activity_rows_inserted = result['status'].split(" ")[2] | int %}
    {% endif %}

    {%- set sum_rows_inserted = loop_vars['sum_rows_inserted'] + activity_rows_inserted -%}
    {%- do loop_vars.update({'sum_rows_inserted': sum_rows_inserted}) -%}
    {{ log(activity_name ~ ': total rows inserted - ' ~ activity_rows_inserted) }}


    {%- set msg = i ~ " of " ~ (num_activities) ~ " " ~ activity_name ~ " - " ~ activity_rows_inserted ~ " records inserted" -%}
    {{ log(msg, info=True) }}
  {%- endfor -%}
  {{ log('target: '~load_relation(target_relation)) }}
  {{ log('tmp: '~tmp_relation) }}
  {%- if full_refresh -%}
  {# if full_refresh, then swap existing table with temp table #}
    {{ adapter.rename_relation(old_relation, backup_relation) }}
    {{ adapter.rename_relation(tmp_relation, target_relation) }}
    {{ adapter.drop_relation(backup_relation) }}
    {{ adapter.drop_relation(tmp_relation) }}
  {{ log('done with drops') }}
  {%- endif -%}
  {%- set end_msg = "Inserted " ~ loop_vars['sum_rows_inserted'] ~ " rows into activity stream " ~ identifier -%}
  {{ log(end_msg, info=True) }}

  {{ run_hooks(post_hooks) }}
  {{ log('target relation: '~target_relation) }}

  {{ adapter.commit() }}

  {% call statement('commit', fetch_result=True) %}
    -- ensure model is committed before executing recluster and vacuum
    commit;
  {%- endcall -%}

  {% call statement('recluster', fetch_result=True) -%}
    {{ dbt_activity_schema.resume_cluster(target_relation) }}
  {%- endcall %}
  {{ log('reclustered/vacuumed table') }}

  {%- set status_string = "INSERT " ~ loop_vars['sum_rows_inserted'] -%}

  {% call noop_statement('main', status_string) -%}
    {{sql}}
  {%- endcall %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
