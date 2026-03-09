{#
  full_table_null_check(model)
  ----------------------------
  Returns all rows where ANY column is NULL.
  Useful as a data-quality test or for ad-hoc null auditing.

  Usage:
    {{ full_table_null_check(ref('fct_ratings')) }}
#}
{% macro full_table_null_check(model) %}
    {%- set cols = adapter.get_columns_in_relation(model) -%}
    select * from {{ model }}
    where
    {%- for col in cols %}
        {{ col.column }} is null
        {%- if not loop.last %} or {% endif %}
    {%- endfor %}
{% endmacro %}
