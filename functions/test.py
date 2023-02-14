from functions.connect_to_db import get_database
from geojson import FeatureCollection, Point, Feature
import json

db = get_database()

sample_houses = db['sample houses'].find({})

sample_features = []

for sample_house in list(sample_houses):
    sample_point = Point(coordinates=sample_house['location']['coordinates'])
    sample_features.append(Feature(geometry=sample_point))

sample_fc = FeatureCollection(features=sample_features)

with open('../files/test_dump.json', 'w') as f:
    json.dump(sample_fc, f)
