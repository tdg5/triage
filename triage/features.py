from collate import collate
import sqlalchemy.sql.expression as ex
from itertools import chain
from datetime import date

THIS_YEAR = date.today().year
LAST_5_YEARS = (date(y, 1, 1) for y in range(THIS_YEAR-5, THIS_YEAR)),


def make_sql_clause(s, constructor):
    if not isinstance(s, ex.ClauseElement):
        return constructor(s)
    else:
        return s


class Aggregation(object):
    def __init__(self, aggregates, from_obj, group_by, prefix=None):
        """
        Args:
            aggregates: collection of Aggregate objects
            from_obj: defines the from clause, e.g. the name of the table
            group_by: defines the groupby, e.g. the name of a column
            prefix: name of prefix for column names, defaults to from_obj

        The from_obj and argument is passed directly to the
            SQLAlchemy Select object so could be anything supported there.
            For details see:
            http://docs.sqlalchemy.org/en/latest/core/selectable.html
        """
        self.aggregates = aggregates
        self.from_obj = make_sql_clause(from_obj, ex.table)
        self.group_by = make_sql_clause(group_by, ex.literal_column)
        self.prefix = prefix if prefix else str(from_obj)

    def get_queries(self):
        """
        Constructs select queries for this aggregation

        Returns: one SQLAlchemy Select query object per date
        """
        prefix = "{prefix}_{group_by}_".format(
            prefix=self.prefix, group_by=self.group_by
        )
        queries = []
        columns = list(chain(*(a.get_columns(None, prefix) for a in self.aggregates)))
        queries.append(
            ex.select(columns=columns, from_obj=self.from_obj)
              .group_by(self.group_by)
        )

        return queries


class FeatureGenerator(object):
    functions = ['count', 'sum', 'min', 'max']

    def __init__(self, config):
        self.config = config

    def entity_config(self, entity):
        return next(e for e in self.config['entities'] if e['name'] == entity)

    def attributes_of_types(self, entity_config, statistical_types):
        return [
            attribute
            for attribute, attribute_type
            in entity_config['attributes'].items()
            if attribute_type in statistical_types
        ]

    def entity_aggregates(self, entity):
        values = self.attributes_of_types(self.entity_config(entity), ['real'])
        return [
            collate.Aggregate(value, self.functions)
            for value in values
        ]

    def event_aggregation(self, entity_name, category):
        return collate.SpacetimeAggregation(
            self.entity_aggregates(entity_name),
            ['1 year',  'all'],
            entity_name,
            category,
            LAST_5_YEARS,
            date_column='event_datetime'
        )

    def entity_features(self, entity_name):
        config = self.entity_config(entity_name)
        categories = self.attributes_of_types(
            config,
            ['categorical', 'binary']
        )
        if config.get('event'):
            return [
                self.event_aggregation(entity_name, category)
                for category in categories
            ]
        else:
            return [
                Aggregation(
                    self.entity_aggregates(entity_name),
                    entity_name,
                    category
                )
                for category in categories
            ]

    def primary_entity_features(self):
        return self.entity_features(self.config['primary_entity'])

    def primary_relation_features(self):
        primary = self.config['primary_entity']
        primary_relationships = [
            relation['entity_one'] if relation['entity_two'] == primary else relation['entity_two']
            for relation in self.config['relationships']
            if relation['entity_one'] == primary
            or relation['entity_two'] == primary
        ]
        return [
            self.entity_features(relation)
            for relation in primary_relationships
        ]
