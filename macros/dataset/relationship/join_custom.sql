{% macro join_custom(
    primary_activity,
    primary_cte,
    secondary_activity,
    secondary_alias,
    nth_occurrence=none,
    after_timestamp=none,
    before_timestamp=none
) %}
{% if after_timestamp is not none -%}
{%- set after_timestap_formatted = after_timestamp.format(primary_activity_at=primary_cte~'.'~primary_activity~'_activity_at') -%}
and {{secondary_alias}}.{{secondary_activity}}_activity_at >= {{after_timestamp_formatted}}
{% endif %}
{% if before_timestamp is not none -%}
{%- set before_timestap_formatted = before_timestamp.format(primary_activity_at=primary_cte~'.'~primary_activity~'_activity_at') -%}
and {{secondary_alias}}.{{secondary_activity}}_activity_at <= {{before_timestamp_formatted}}
{% endif %}
{% endmacro %}
