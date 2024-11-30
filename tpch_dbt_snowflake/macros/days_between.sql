{% macro days_between(date1,date2) %}
   
    datediff(day,{{date1}}::date,{{date2}}::date)

{% endmacro %}