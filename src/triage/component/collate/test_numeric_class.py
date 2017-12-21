from jinja2 import Template

class NumericAggregation(Aggregation):
    def __init__(self, intervals,  aggregates, groups, from_obj, dates, state_table, state_group=None, prefix=None, suffix=None, date_column=None, output_date_column=None, input_min_date=None):
    
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

    def define_template(self, groups, intervals, functions, from_obj, prefix):
    """Defines the templates for numeric aggregation. This template is then passed to the database connection object where it is executed in the standard collate format."""
    template =  """
    select {{ group }}, {{ as_of_date }}::date as date
    {% for interval in intervals %}
    {% for function in functions %}
    ,{{ function }}( {{ column_name }} ) FILTER (WHERE date >= {{ as_of_date }}::date - interval {{ interval }}) AS
    "{{ prefix }}_{{ group }}_{{ interval }}_{{ column_name }}"
    {% endfor %}
    {% endfor %}
    FROM {{ from_obj }}
    WHERE date < {{ as_of_date }} AND date >= {{ as_of_date }}::date - greatest(
    {% for interval in intervals %}
    interval {{ interval }},
    {% endfor %} )
    GROUP BY {{ group }}"""

    return template
    
    def define_data(self, groups, intervals, functions, from_obj, prefix, as_of_date):
        pass

    def render_template(self, template):
    t = Template(template)
    return t

    def execute_db(self, t):
    db_conn.execute(t.render(data))




  
