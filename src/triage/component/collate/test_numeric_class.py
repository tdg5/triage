from jinja2 import Template

class NumericAggregation(Aggregation):
    def __init__(self, intervals, column_name, aggregates, groups, from_obj, dates, state_table, state_group=None, prefix=None, suffix=None, date_column=None, output_date_column=None, input_min_date=None):
    
    self.intervals = intervals

    Aggregation.__init__(self, 
                        aggregates=aggregates,
                        from_obj=from_obj,
                        groups=groups,
                        state_table=state_table,
                        state_group=state_group,
                        prefix=prefix,
                        suffix=suffix,
                        schema=schema)

    def define_template(self, groups, intervals, functions, column_name, prefix):
    """Defines the templates for numeric aggregation. This template is then passed to the database connection object where it is executed in the standard collate format."""
    return """
    select {{ group }}, {{ as_of_date }}::date as date
    {% for interval in intervals %}
    {% for function in functions %}
    ,{{ function }}( {{ column_name }} ) FILTER (WHERE date >= {{ as_of_date }}::date - interval {{ interval }}) AS
    "{{ prefix }}_{{ group }}_{{ interval }}_{{ column_name }}"
    {% endfor %}
    {% endfor %}
    FROM {{ table_name }}
    WHERE date < {{ as_of_date }} AND date >= {{ as_of_date }}::date - greatest(
    {% for interval in intervals %}
    interval {{ interval }},
    {% endfor %} )
    GROUP BY {{ group }}"""

    def render_template(self):


  
