{%- macro compile_relationship_join(
    primary_activity,
    primary_cte,
    secondary_activity,
    secondary_alias,
    relationship,
    nth_occurrence=none,
    after_timestamp=none,
    before_timestamp=none
) -%}

{%- set relationships = dbt_activity_schema._get_relationships() -%}
{%- set relationship_join = relationships.get(relationship)(
    primary_activity=primary_activity,
    primary_cte=primary_cte,
    secondary_activity=secondary_activity,
    secondary_alias=secondary_alias,
    nth_occurrence=nth_occurrence,
    after_timestamp=after_timestamp,
    before_timestamp=before_timestamp
)-%}
{{relationship_join}}

{%- endmacro -%}
