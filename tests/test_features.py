from triage.schema import InspectionsSchema
from triage.features import FeatureGenerator
from collate import collate
import yaml


with open('tests/example_schema.yaml') as stream:
    CONFIG = yaml.load(stream)
schema = InspectionsSchema(CONFIG)


def test_primary_entity_features():
    feature_gen = FeatureGenerator(CONFIG)
    assert len(feature_gen.primary_entity_features()) == 1


def test_primary_relation_features():
    feature_gen = FeatureGenerator(CONFIG)
    relations = feature_gen.primary_relation_features()
    assert len(relations) == 1  # one for each relation
    assert len(relations[0]) == 2  # one for each categorical variable


def test_entity_aggregates():
    feature_gen = FeatureGenerator(CONFIG)
    assert len(feature_gen.entity_aggregates('kid')) == 1


def test_entity_features_event():
    feature_gen = FeatureGenerator(CONFIG)
    result = feature_gen.entity_features('inspection')
    assert len(result) == 1
    assert type(result[0]) == collate.SpacetimeAggregation
