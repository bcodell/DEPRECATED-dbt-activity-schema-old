{% macro join_nth_ever(
    primary_activity,
    primary_cte,
    secondary_activity,
    secondary_alias,
    nth_occurrence=none,
    after_timestamp=none,
    before_timestamp=none
) %}
and {{secondary_alias}}.{{secondary_activity}}_activity_occurrence = {{nth_occurrence}}
{% endmacro %}
