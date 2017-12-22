# coding: utf-8

import jinja2

with open("numeric_template.sql") as f:
    template = f.read()

FUNCTIONS = ['avg', 'sum', 'stddev', 'max', 'min']

class NumericSpaceTimeAggregation(object):
    def __init__(self, prefix, numeric_columns, from_obj, intervals, as_of_dates, groups):

        self.prefix=prefix
        self.numeric_columns=numeric_columns
        self.from_obj=from_obj
        self.intervals=intervals
        self.as_of_dates=as_of_dates
        self.groups=groups

        self.data={"numeric_column": self.numeric_columns[0], 
                    "functions": FUNCTIONS,
                    "prefix": self.prefix,
                    "table_name": self.from_obj,
                    "as_of_date": self.as_of_dates[0],
                    "group": self.groups[0],
                    "intervals": self.intervals}
 
        self.template = jinja2.Template(template)

        self.query = self.template.render(self.data)
    
    def execute(self, conn):
        conn.execute(self.query)


