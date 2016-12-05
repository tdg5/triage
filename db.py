import yaml
from sqlalchemy import create_engine


def get_engine():
    with open('database.yaml', 'r') as f:
        config = yaml.load(f)
    url = "postgresql://{user}:{pass}@{host}:{port}/{db}".format(**config)
    return create_engine(url)
