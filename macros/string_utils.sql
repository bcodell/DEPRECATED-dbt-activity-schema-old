{%- macro hash(field) -%}
  {{ return(adapter.dispatch('hash', 'dbt_activity_schema') (field)) }}
{%- endmacro -%}

{%- macro default__hash(field) -%}
    md5(cast({{field}} as {{type_string()}}))
{%- endmacro -%}

{%- macro bigquery__hash(field) -%}
    to_hex({{dbt_activity_schema.default__hash(field)}})
{%- endmacro -%}



{%- macro concat(fields) -%}
  {{ return(adapter.dispatch('concat', 'dbt_activity_schema')(fields)) }}
{%- endmacro -%}

{%- macro default__concat(fields) -%}
    {{ fields|join(' || ') }}
{%- endmacro -%}

{%- macro surrogate_key(field_list) -%}
    {# needed for safe_add to allow for non-keyword arguments see SO post #}
    {# https://stackoverflow.com/questions/13944751/args-kwargs-in-jinja2-macros #}
    {%- set frustrating_jinja_feature = varargs -%}
    {{ return(adapter.dispatch('surrogate_key', 'dbt_activity_schema')(field_list, *varargs)) }}
{%- endmacro -%}



{%- macro default__surrogate_key(field_list) -%}

{%- if varargs|length >= 1 or field_list is string -%}

{%- set error_message = '
Warning: the `surrogate_key` macro now takes a single list argument instead of \
multiple string arguments. Support for multiple string arguments will be \
deprecated in the future. The {}.{} model triggered this warning. \
'.format(model.package_name, model.name) -%}

{%- do exceptions.warn(error_message) -%}

{# first argument is not included in varargs, so add first element to field_list_xf #}
{%- set field_list_xf = [field_list] -%}

{%- for field in varargs -%}
{%- set _ = field_list_xf.append(field) -%}
{%- endfor -%}

{%- else -%}

{# if using list, just set field_list_xf as field_list #}
{%- set field_list_xf = field_list -%}

{%- endif -%}


{%- set fields = [] -%}

{%- for field in field_list_xf -%}

    {%- set _ = fields.append(
        "coalesce(cast(" ~ field ~ " as " ~ type_string() ~ "), '')"
    ) -%}

    {%- if not loop.last -%}
        {%- set _ = fields.append("'-'") -%}
    {%- endif -%}

{%- endfor -%}

{{dbt_activity_schema.hash(dbt_activity_schema.concat(fields))}}

{%- endmacro -%}

{%- macro remove_punctuation(s) -%}
  {{ return(adapter.dispatch('remove_punctuation', 'dbt_activity_schema')(s)) }}
{%- endmacro -%}

{%- macro default__remove_punctuation(s) -%}
{%- set open_bracket = '[' -%}
{%- set close_bracket = ']' -%}
{%- set punc = ['!', ' ', '"', '#', '$', '%', '&', "'", '(', ')', '*', '+', ',', '-', '.', '/', ':', ';', '<', '=', '>', '?', '@', '\\', '^', '`', '{', '|', '}', '~', '{{open_bracket}}', '{{close_bracket}}'] -%}
{%- set clean_str = [s|string] -%}
{%- for p in punc -%}
{%- do clean_str.append(clean_str[-1].replace(p,'')) -%}
{%- endfor -%}
{{clean_str[-1]}}
{%- endmacro -%}


{% macro remove_prefix(activity_name) %}
  {{ return(adapter.dispatch('remove_prefix', 'dbt_activity_schema')(activity_name)) }}
{% endmacro %}

{% macro default__remove_prefix(activity_name) %}
{%- set split_activity = activity_name.split('__') -%}
{%- if split_activity|length > 1 -%}
{{ return(activity_name.split('__')[1:]|join('')) }}
{%- else -%}
{{ return(activity_name) }}
{%- endif -%}
{% endmacro %}


{%- macro select_columns(column_list, indent_spaces=8) -%}
    {{ adapter.dispatch('select_columns', 'dbt_activity_schema')(column_list, indent_spaces=8) }}
{%- endmacro -%}

{%- macro default__select_columns(column_list, indent_spaces=8) -%}
{% for column in column_list -%}
{%- if not loop.first -%}{% for s in range(indent_spaces) %} {% endfor %}{%- endif -%}, {{column}}
{% endfor %}
{%- endmacro -%}
