import json
import random
from typing import Dict, Any
from turfpy.measurement import bbox, boolean_point_in_polygon
from turfpy.random import random_position
from geojson import Polygon, Feature, LineString, FeatureCollection, Point, MultiPolygon
from turfpy.transformation import transform_translate, line_offset
from random import randint
from functions.connect_to_db import get_database
from tqdm import tqdm
import gspread

gc = gspread.service_account(filename='../files/credentials.json')

sh = gc.open_by_key('1INGKUPbuG252SgYVGpTHGXuEE_41V1evgaTOIZ0bix4')
loss_ws = sh.worksheet('Losses')

us_poly_file = open('../files/us_poly.json')
ef_matrix_file = open('../files/ef_matrix.json')
state_ef_matrix_file = open('../files/state_ef_matrix.json')
torn_lengths_file = open('../files/torn_lengths.json')
state_polys_file = open('../files/statepolys.json')

ef_matrix = json.load(ef_matrix_file)
state_ef_matrix = json.load(state_ef_matrix_file)
torn_lengths = json.load(torn_lengths_file)
state_polys = json.load(state_polys_file)
us_poly = json.load(us_poly_file)

db = get_database()

avg_payout_dict = {
    "EF0": 2000,
    "EF1": 2000,
    "EF2": 3000,
    "EF3": 4125,
    "EF4": 5300,
    "EF5": 6915,
}

width_dict = {
    "EF0": 1,
    "EF1": 2,
    "EF2": 3,
    "EF3": 4,
    "EF4": 5,
    "EF5": 6
}

width = 50


def generate_sample_tornado() -> dict[str, Any]:
    """ Generates a state and a severity based on historic tornado occurrences
    """
    sev_choices = list(ef_matrix.keys())
    sev_weights = list(ef_matrix.values())

    severity = random.choices(sev_choices, k=1, weights=sev_weights)[0]

    state_choices = list(state_ef_matrix[severity].keys())
    state_weights = list(state_ef_matrix[severity].values())

    state = random.choices(state_choices, k=1, weights=state_weights)[0]

    len_choices = list(torn_lengths.keys())
    len_weights = list(torn_lengths.values())
    length = random.choices(len_choices, k=1, weights=len_weights)[0]

    return {
        "severity": severity,
        "state": state,
        "length": length
    }


def generate_tornado_track():
    tornado_data = generate_sample_tornado()

    angle = randint(20, 70)

    state = tornado_data["state"]
    severity = tornado_data["severity"]
    length = tornado_data["length"]

    state_poly = state_polys[state]
    state_bbox = bbox(state_poly)

    us_feature = Feature(geometry=us_poly)

    torn_center = Feature(geometry=Point(coordinates=random_position(state_bbox)))

    if not boolean_point_in_polygon(torn_center, us_feature):
        return False

    torn_start = transform_translate(torn_center, int(int(length) / 2), direction=angle, mutate=False)
    torn_end = transform_translate(torn_center, int(int(length) / 2), direction=angle + 180, mutate=False)

    center_line = Feature(
        geometry=LineString([torn_start['geometry']['coordinates'], torn_end['geometry']['coordinates']]))
    torn_width = ((width_dict[severity] * width) / 2) / 1760

    top_line = line_offset(center_line, torn_width, unit="mi")['geometry']['coordinates']
    bottom_line = line_offset(center_line, torn_width * -1, unit="mi")['geometry']['coordinates']
    bottom_line.reverse()
    torn_poly_start = top_line[0]

    torn_coordinates = (top_line + bottom_line + [torn_poly_start])

    torn_poly_geom = Polygon(coordinates=[torn_coordinates])

    tornado_poly = Feature(geometry=torn_poly_geom, properties={
        "payout": avg_payout_dict[severity],
        "severity": severity
    })

    return tornado_poly


def run_simulation(tornado_group_mod):
    tracks = []
    loss_dictionary = {
        "total": 0,
        "state": {
            "KY": 0,
            "MS": 0,
            "OK": 0,
            "TN": 0,
            "IL": 0,
            "NE": 0,
            "OH": 0,
            "GA": 0,
            "KS": 0,
            "AL": 0
        },
        "county": {
            "Todd County": 0,
            "Christian County": 0,
            "Simpson County": 0,
            "Osage County": 0,
            "Pawnee County": 0,
            "Hart County": 0,
            "Barren County": 0,
            "Lauderdale County": 0,
            "Muskogee County": 0,
            "Cherokee County": 0,
            "Madison County": 0,
            "Greenup County": 0,
            "Clinton County": 0,
            "Douglas County": 0,
            "Cuming County": 0,
            "Burt County": 0,
            "Campbell County": 0,
            "Hamilton County": 0,
            "Kenton County": 0,
            "Berrien County": 0,
            "Cook County": 0,
            "Tift County": 0,
            "Colquitt County": 0,
            "Sarpy County": 0,
            "Dawson County": 0,
            "Lincoln County": 0,
            "Pearl River County": 0,
            "Sumner County": 0,
            "Robertson County": 0,
            "Davidson County": 0,
            "Williamson County": 0,
            "Warren County": 0,
            "McPherson County": 0,
            "Coahoma County": 0,
            "Johnson County": 0,
            "Coffee County": 0,
            "Knox County": 0,
            "McHenry County": 0,
            "Oglethorpe County": 0,
            "Ottawa County": 0,
            "Erie County": 0,
            "Lancaster County": 0
        },
        "zip": {
            "30619": 0,
            "30630": 0,
            "30648": 0,
            "30667": 0,
            "31637": 0,
            "31647": 0,
            "31775": 0,
            "31793": 0,
            "31794": 0,
            "36330": 0,
            "37013": 0,
            "37027": 0,
            "37067": 0,
            "37069": 0,
            "37148": 0,
            "37204": 0,
            "37211": 0,
            "37215": 0,
            "37218": 0,
            "37220": 0,
            "38305": 0,
            "38614": 0,
            "38617": 0,
            "38645": 0,
            "39114": 0,
            "39305": 0,
            "39307": 0,
            "39320": 0,
            "39426": 0,
            "39466": 0,
            "41011": 0,
            "41014": 0,
            "41015": 0,
            "41016": 0,
            "41017": 0,
            "41018": 0,
            "41071": 0,
            "41072": 0,
            "41073": 0,
            "41074": 0,
            "41075": 0,
            "41076": 0,
            "41085": 0,
            "41102": 0,
            "41121": 0,
            "41139": 0,
            "41144": 0,
            "42101": 0,
            "42103": 0,
            "42104": 0,
            "42122": 0,
            "42127": 0,
            "42134": 0,
            "42220": 0,
            "42221": 0,
            "42240": 0,
            "42266": 0,
            "42749": 0,
            "43433": 0,
            "43440": 0,
            "43452": 0,
            "44870": 0,
            "45202": 0,
            "45203": 0,
            "45204": 0,
            "45206": 0,
            "45208": 0,
            "45226": 0,
            "45230": 0,
            "60012": 0,
            "60013": 0,
            "60014": 0,
            "60098": 0,
            "60102": 0,
            "60142": 0,
            "60156": 0,
            "61401": 0,
            "61428": 0,
            "61430": 0,
            "61436": 0,
            "61448": 0,
            "62218": 0,
            "62219": 0,
            "62230": 0,
            "62231": 0,
            "66030": 0,
            "66031": 0,
            "66061": 0,
            "66062": 0,
            "67443": 0,
            "67456": 0,
            "67460": 0,
            "68004": 0,
            "68038": 0,
            "68045": 0,
            "68107": 0,
            "68117": 0,
            "68142": 0,
            "68147": 0,
            "68157": 0,
            "68164": 0,
            "68502": 0,
            "68503": 0,
            "68504": 0,
            "68505": 0,
            "68506": 0,
            "68507": 0,
            "68510": 0,
            "68512": 0,
            "68516": 0,
            "68521": 0,
            "68788": 0,
            "69138": 0,
            "74020": 0,
            "74035": 0,
            "74434": 0
        }
    }

    num_tornadoes = 1
    limit = randint(200, 1000)
    while num_tornadoes <= limit:

        track = generate_tornado_track()
        if not track:
            pass
        else:
            tracks.append(track)
            num_tornadoes += 1

    multi_tornado_dict = {
        "EF0": {},
        "EF1": {},
        "EF2": {},
        "EF3": {},
        "EF4": {},
        "EF5": {}
    }

    for severity in multi_tornado_dict.keys():
        same_sev_coordinates = [sev_track['geometry']['coordinates'] for sev_track in tracks if
                                sev_track['properties']['severity'] == severity]
        sev_multi = MultiPolygon(coordinates=same_sev_coordinates)
        multi_tornado_dict[severity] = Feature(geometry=sev_multi)

    with open(f'../files/tornado_collections/tornado_tracks.json', 'w', encoding='utf-8') as f:
        json.dump(multi_tornado_dict, f)

    tornado_group_mod.insert_one(multi_tornado_dict)

    for severity, torn_poly in multi_tornado_dict.items():
        if severity not in avg_payout_dict:
            continue
        if len(torn_poly['geometry']['coordinates']) > 0:
            damaged_homes_col = db["sample houses"].find({
                "location": {
                    "$geoWithin": {
                        "$geometry": {
                            "type": torn_poly['geometry']['type'],
                            "coordinates": torn_poly['geometry']['coordinates']
                        }
                    }
                }
            })

            damaged_homes = list(damaged_homes_col)

            payout = avg_payout_dict[severity]

            for damaged_home in damaged_homes:
                loss_dictionary['total'] += payout
                loss_dictionary['state'][damaged_home['state']] += payout
                loss_dictionary['county'][damaged_home['county']] += payout
                loss_dictionary['zip'][damaged_home['zip_code']] += payout
    loss_list = []

    for key, value in loss_dictionary.items():
        if key == 'total':
            loss_list.append(value)
        else:
            for key2, value2 in value.items():
                loss_list.append(value2)

    loss_ws.append_row(loss_list)


tornado_group = db['tornado_group']

for i in tqdm(range(48887)):
    run_simulation(tornado_group)
