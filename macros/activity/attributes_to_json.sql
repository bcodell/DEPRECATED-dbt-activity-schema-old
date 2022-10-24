{%- macro attributes_to_json(attributes_list) -%}
    {{ adapter.dispatch('attributes_to_json', 'dbt_activity_schema')(attributes_list) }}
{%- endmacro -%}


{%- macro default__attributes_to_json(attributes_list) -%}
    {%- if attributes_list is not none -%}
    {%- set attributes = attributes_list.keys() -%}
    json_build_object(
        {% for attr in attributes -%}
        '{{attr}}', {{attr}}{% if not loop.last -%},{% endif %}
        {% endfor -%}
    )
    {%- else -%}
    cast(null as json)
    {%- endif -%}
{%- endmacro -%}


{%- macro postgres__attributes_to_json(attributes_list) -%}
    {{ log('test attr' ~ attributes_list) }}
    {%- if attributes_list is not none -%}
    {%- set attributes = attributes_list.keys() -%}
    json_build_object(
        {% for attr in attributes -%}
        '{{attr}}', {{attr}}{% if not loop.last -%},{% endif %}
        {% endfor -%}
    )
    {%- else -%}
    cast(null as json)
    {%- endif -%}
{%- endmacro -%}


{%- macro redshift__attributes_to_json(attributes_list) -%}
    {%- if attributes_list is not none -%}
    {%- set attributes = attributes_list.keys() -%}
    '{' ||
        {% for attr in attributes -%}
        {% if not loop.first -%}', '{%- endif -%}'"{{attr}}": "' || decode(cast({{attr}} as {{type_string()}}), null, '', cast({{attr}} as {{type_string()}})){% if not loop.last %} ||{% endif %}
        {% endfor -%}
    || '}'
    {%- else -%}
    cast(null as varchar)
    {%- endif -%}
{%- endmacro -%}


{%- macro bigquery__attributes_to_json(attributes_list) -%}
    {%- if attributes_list is not none -%}
    {%- set attributes = attributes_list.keys() -%}
    to_json(struct(
        {% for attr in attributes -%}
        {{attr}} as {{attr}}{%- if not loop.last -%},{% endif %}
        {% endfor -%}
    ))
    {%- else -%}
    null
    {%- endif -%}
{%- endmacro -%}


{%- macro snowflake__attributes_to_json(attributes_list) -%}
    {%- if attributes_list is not none -%}
    {%- set attributes = attributes_list.keys() -%}
    object_construct(
        {% for attr in attributes -%}
        '{{attr}}', {{attr}}{%- if not loop.last -%},{% endif %}
        {% endfor -%}
    )
    {%- else -%}
    null::object
    {%- endif -%}
{%- endmacro -%}
