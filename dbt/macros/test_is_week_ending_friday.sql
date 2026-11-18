{#
  Walmart reports sales by week ending Friday. Fails for any non-null value that is not a Friday.
#}
{% test is_week_ending_friday(model, column_name) %}
select {{ column_name }}
from {{ model }}
where {{ column_name }} is not null
  and {{ walmart_analytics.day_of_week_iso(column_name) }} <> 5
{% endtest %}
