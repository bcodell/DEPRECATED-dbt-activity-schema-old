{%- macro early_timestamp() -%}
  {{ return(adapter.dispatch('early_timestamp', 'dbt_activity_schema')()) }}
{%- endmacro -%}

{%- macro default__early_timestamp() -%}
    cast('100-01-01' as {{dbt_activity_schema.type_timestamp()}})
{%- endmacro -%}


{%- macro compile_timestamp_join(primary_activity, secondary_activity, secondary_alias, relative, timestamp) -%}
    {{ adapter.dispatch('compile_timestamp_join', 'dbt_activity_schema')(primary_activity, secondary_activity, secondary_alias, relative, timestamp) }}
{%- endmacro -%}


{%- macro default__compile_timestamp_join(primary_activity, secondary_activity, secondary_alias, relative, timestamp) -%}
{%- if timestamp is not none -%}
{%- if relative == 'after' -%}
{%- if timestamp == 'previous' -%}
{{secondary_alias}}.{{secondary_activity}}_activity_at >= coalesce(enriched.previous_{{primary_activity}}_activity_at, {{dbt_activity_schema.early_timestamp()}})
{%- elif timestamp == 'current' -%}
{{secondary_alias}}.{{secondary_activity}}_activity_at > coalesce(enriched.{{primary_activity}}_activity_at, {{dbt_activity_schema.early_timestamp()}})
{%- elif timestamp not in ['after', 'previous', 'current'] -%}
{{secondary_alias}}.{{secondary_activity}}_activity_at >= {{timestamp}}
{%- endif -%}
{%- elif relative == 'before' -%}
{%- if timestamp == 'next' -%}
{{secondary_alias}}.{{secondary_activity}}_activity_at <= coalesce(enriched.next_{{primary_activity}}_activity_at, {{current_timestamp()}})
{%- elif timestamp == 'current' -%}
{{secondary_alias}}.{{secondary_activity}}_activity_at < coalesce(enriched.{{primary_activity}}_activity_at, {{current_timestamp()}})
{%- elif timestamp not in ['after', 'previous', 'current'] -%}
{{secondary_alias}}.{{secondary_activity}}_activity_at <= {{timestamp}}
{%- endif -%}
{%- endif -%}
{%- endif -%}

{%- endmacro -%}
