-- macros/exclude_column.sql
{% macro exclude_column(relation, excluded_column) %}
    {% set columns = dbt_utils.get_columns_in_relation(relation) %}
    {% set filtered_columns = columns | reject('equalto', excluded_column) %}
    {{ return(filtered_columns | join(', ')) }}
{% endmacro %}
