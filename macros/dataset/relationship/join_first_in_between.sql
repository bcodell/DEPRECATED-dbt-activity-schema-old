{% macro join_first_in_between(
    primary_activity,
    primary_cte,
    secondary_activity,
    secondary_alias,
    nth_occurrence=none,
    after_timestamp=none,
    before_timestamp=none
) %}
and {{secondary_alias}}.{{secondary_activity}}_activity_at > {{primary_cte}}.{{primary_activity}}_activity_at
and (
    {{secondary_alias}}.{{secondary_activity}}_activity_at <= {{primary_cte}}.{{primary_activity}}_activity_repeated_at
    or {{primary_cte}}.{{primary_activity}}_activity_repeated_at is null
)
{% endmacro %}
