{% macro build_activity_stream(activity_list) %}
{{ adapter.dispatch('build_activity_stream', 'dbt_activity_schema')(activity_list)}}
{% endmacro %}

{% macro default__build_activity_stream(activity_list) %}

{% for activity in activity_list %}
{{"-- depends_on: "~activity}}
{% endfor %}
{% endmacro %}
