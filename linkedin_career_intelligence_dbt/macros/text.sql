{% macro as_varchar(expression) -%}
    cast({{ expression }} as varchar)
{%- endmacro %}

{% macro trim_text(expression) -%}
    trim({{ as_varchar(expression) }})
{%- endmacro %}

{% macro nullif_trim_text(expression) -%}
    nullif({{ trim_text(expression) }}, '')
{%- endmacro %}

{% macro lower_trim_text(expression) -%}
    lower({{ trim_text(expression) }})
{%- endmacro %}

{% macro upper_trim_text(expression) -%}
    upper({{ trim_text(expression) }})
{%- endmacro %}
