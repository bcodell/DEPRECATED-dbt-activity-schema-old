{% macro join_aggregate_after(
    primary_activity,
    primary_cte,
    secondary_activity,
    secondary_alias,
    nth_occurrence=none,
    after_timestamp=none,
    before_timestamp=none
) %}
and {{secondary_alias}}.{{secondary_activity}}_activity_at > {{primary_cte}}.{{primary_activity}}_activity_at
{% endmacro %}
