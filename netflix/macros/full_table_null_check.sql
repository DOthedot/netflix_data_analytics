{% macro full_table_null_check(model) %}
    select * from {{ model }} where 
    {% for col in adapter.get_columns_in_relation(model) %}
        {{ col.column }} is null or 
        {% endfor %}
        false
{% endmacro %}