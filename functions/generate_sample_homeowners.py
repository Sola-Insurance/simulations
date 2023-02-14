from functions.connect_to_db import get_database
from tqdm import tqdm
import random
import json
import requests
from turfpy.measurement import points_within_polygon, boolean_point_in_polygon
from geojson import Feature, Point, FeatureCollection
from turfpy.transformation import circle

db = get_database()

states = ['AL', 'AR', 'GA', 'IA', 'IL', 'IN', 'KS', 'KY', 'MO', 'MS', 'NE', 'OH', 'OK', 'TN', 'TX']

limit_options = [
    2000,
    3000,
    4000,
    5000,
    6000,
    7000,
    8000,
    9000,
    10000,
    11000,
    12000,
    13000,
    14000,
    15000
]

sample_houses = db["sample houses"]

total_agg_limit = 200000000
state_agg_limit = 80000000
county_agg_limit = 30000000
zip_agg_limit = 5000000


def generate_homeowners(num_homeowners):
    agencies = 0
    sample_houses.delete_many({})
    sample_homes = []
    total = 0
    total_agg = 0
    zip_dict = {}
    county_dict = {}
    state_dict = {}
    while total < num_homeowners:

        agencies += 1
        print("Curr Total", total)

        curr_state = random.choice(states)

        potential_locations_file = open(f'../files/state_locations/{curr_state}_points.json')
        potential_locations = json.load(potential_locations_file)['features']

        location = random.choice(potential_locations)

        rand_radius = random.randint(1, 10)

        location_bound = circle(location, radius=rand_radius, steps=6)

        near_locations = []
        for potential_location in tqdm(potential_locations):
            if boolean_point_in_polygon(potential_location, location_bound):
                near_locations.append(potential_location)

        for near_location in tqdm(near_locations):
            try:
                sample_limit = random.choice(limit_options)
                sample_lat = near_location['geometry']['coordinates'][1]
                sample_long = near_location['geometry']['coordinates'][0]

                geocode_url = f"https://maps.googleapis.com/maps/api/geocode/json?latlng={sample_lat},{sample_long}&key=AIzaSyAtJhfbmgadiIzvuN7KZ0ryx50k6VLb4YE"

                address_data = requests.get(geocode_url).json()['results'][0]['address_components']

                state = [address_component['short_name'] for address_component in address_data if
                         'administrative_area_level_1' in address_component['types']][0]
                county = [address_component['short_name'] for address_component in address_data if
                          'administrative_area_level_2' in address_component['types']][0]
                zip_code = [address_component['short_name'] for address_component in address_data if
                            'postal_code' in address_component['types']][0]

                if state not in state_dict:
                    state_dict[state] = 0
                if county not in county_dict:
                    county_dict[county] = 0
                if zip_code not in zip_dict:
                    zip_dict[zip_code] = 0

                if (total_agg + sample_limit) >= total_agg_limit or (state_dict[state] + sample_limit) >= state_agg_limit or (county_dict[county] + sample_limit) >= county_agg_limit or (zip_dict[zip_code] + sample_limit) >= zip_agg_limit:
                    print("Agg reached")
                    break

                total_agg += sample_limit
                state_dict[state] += sample_limit
                county_dict[county] += sample_limit
                zip_dict[zip_code] += sample_limit

                sample_home = {
                    "state": state,
                    "county": county,
                    "zip_code": zip_code,
                    "limit": sample_limit,
                    "location": near_location['geometry']
                }
                sample_homes.append(sample_home)
                total += 1
            except Exception as e:
                pass
            if total >= num_homeowners:
                break

        if total_agg >= (total_agg_limit - 15000):
            break

    sample_houses.insert_many(sample_homes)
    print("Zips", zip_dict)
    print("Counties", county_dict)
    print("States", state_dict)
    print("Agents", agencies)


if __name__ == '__main__':
    generate_homeowners(25000)
