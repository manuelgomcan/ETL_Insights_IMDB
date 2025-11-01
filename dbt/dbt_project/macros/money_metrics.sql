{# Macro para limpiar y convertir campos monetarios a numeric #}
{% macro clean_money(column_name) %}
    coalesce(cast(nullif({{ column_name }}, '') as numeric), 0)
{% endmacro %}
