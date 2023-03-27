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
and {{secondary_alias}}.{{secondary_activity}}_activity_at >= {{after_timestamp}}
{% endif %}
{% if before_timestamp is not none -%}
and {{secondary_alias}}.{{secondary_activity}}_activity_at <= {{before_timestamp}}
{% endif %}
{% endmacro %}
