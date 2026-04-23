{% macro incremental_filter(column_name) %}
    {% if is_incremental() %}
        WHERE {{ column_name }} > (SELECT MAX({{ column_name }}) FROM {{ this }})
    {% endif %}
{% endmacro %}