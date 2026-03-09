{#
  generate_schema_name
  --------------------
  In production (target.name == 'prod') schemas are used as-is.
  In other environments the default schema is prefixed so that
  dev/CI runs never pollute the production schema.

  e.g. target schema "staging" in dev → "<dev_user>_staging"
       target schema "staging" in prod → "staging"
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
