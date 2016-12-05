from triage.schema import InspectionsSchema
from triage.features import FeatureGenerator
import yaml
import unittest
import db


class DbTestCase(unittest.TestCase):
    def setUp(self):
        self.engine = db.get_engine()
        self.connection = self.engine.connect()
        self.transaction = self.connection.begin()

    def tearDown(self):
        self.transaction.rollback()
        self.transaction.close()


class IntegrationTest(DbTestCase):
    def load_kids(self):
        for values in [
            { 'id': 1, 'dcfs': True, 'bll': 4.25 },
            { 'id': 2, 'dcfs': False, 'bll': 5.25 },
            { 'id': 3, 'dcfs': True, 'bll': 6.25 },
            { 'id': 4, 'dcfs': False, 'bll': 7.25 },
        ]:
            self.connection.execute(
                self.schema.models['kids'].insert().values(**values)
            )

    def load_data(self):
        self.load_kids()

    def test_integration(self):
        with open('tests/example_schema.yaml') as stream:
            CONFIG = yaml.load(stream)
        self.schema = InspectionsSchema(CONFIG)
        self.schema.metadata.drop_all(self.engine)
        self.schema.metadata.create_all(self.engine)
        self.load_data()
        feature_generator = FeatureGenerator(CONFIG)
        primary_features = feature_generator.primary_entity_features()
        self.assertEquals(len(primary_features), 1)
        for query in primary_features[0].get_queries():
            # only one category, so we expect one query
            result = list(self.connection.execute(query))
            self.assertEquals(len(result), 2)
            for row in result:
                print(row)
        self.assertEquals(len(primary_features), 2)
