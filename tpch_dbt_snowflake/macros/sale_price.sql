{% macro sale_price(price,discount,tax) %}

    {{price}}*(1-{{discount}}+{{tax}})::DECIMAL(10,2)

{% endmacro %}