import json
import random
import copy
from multiprocessing import Pool
from typing import Dict, Any
from turfpy.measurement import bbox, boolean_point_in_polygon, points_within_polygon
from turfpy.random import random_position
from geojson import Polygon, Feature, LineString, FeatureCollection, Point, MultiPolygon
from turfpy.transformation import transform_translate, line_offset
from random import randint
from functions.connect_to_db import get_database
from tqdm import tqdm
import time
import gspread
import csv

gc = gspread.service_account(filename='../files/credentials.json')

states = ['AL', 'AR', 'GA', 'IA', 'IL', 'IN', 'KS', 'KY', 'MO', 'MS', 'NE', 'OH', 'OK', 'TN', 'TX']

# sh = gc.open_by_key('1INGKUPbuG252SgYVGpTHGXuEE_41V1evgaTOIZ0bix4')
# loss_ws = sh.worksheet('New Losses')
# exposure_ws = sh.worksheet('Exposures')
# premium_ws = sh.worksheet('Premium')
# nlr_ws = sh.worksheet('NLR')

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

total_agg_limit = 200000000
state_agg_limit = 80000000
county_agg_limit = 30000000
zip_agg_limit = 5000000


def limit_to_premium(limit):
    return round(((((limit - 2000) / 13000) * 20) + 40) * 0.7, 2)


def generate_loss_ratio(premium, loss):
    if premium != 0:
        return round(loss / premium, 4)
    return 0


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


def upload_csv(upload_list, csv_path):
    with open(csv_path, 'a') as f:
        writer = csv.writer(f)
        writer.writerow(upload_list)


def upload_data(upload_list, ws):
    try:
        ws.append_row(upload_list)
    except Exception as e:
        print(e)
        time.sleep(1)
        upload_data(upload_list, ws)


def run_simulation(test):
    tracks = []
    loss_dictionary = {
        "total": 0,
        "state": {'AL': 0, 'AR': 0, 'TX': 0, 'MO': 0, 'OK': 0, 'GA': 0, 'IA': 0, 'IL': 0, 'IN': 0, 'KS': 0, 'KY': 0,
                  'OH': 0, 'TN': 0, 'MS': 0, 'NE': 0},
        "county": {'Crenshaw County': 0, 'Covington County': 0, 'Montgomery County': 0, 'Coffee County': 0,
                   'Jefferson County': 0, 'Madison County': 0, 'Dallas County': 0, 'Chilton County': 0,
                   'Autauga County': 0, 'Butler County': 0, 'Etowah County': 0, 'Mobile County': 0, 'Shelby County': 0,
                   'Baldwin County': 0, 'Escambia County': 0, 'Wilcox County': 0, 'Elmore County': 0, 'Lee County': 0,
                   'Dale County': 0, 'Miller County': 0, 'Bowie County': 0, 'Poinsett County': 0,
                   'Crittenden County': 0, 'Marion County': 0, 'Taney County': 0, 'Boone County': 0, 'Ozark County': 0,
                   'Washington County': 0, 'Benton County': 0, 'Grant County': 0, 'Hot Spring County': 0,
                   'White County': 0, 'Lonoke County': 0, 'Union County': 0, 'Van Buren County': 0, 'Stone County': 0,
                   'Desha County': 0, 'Conway County': 0, 'Pope County': 0, 'Newton County': 0, 'Sebastian County': 0,
                   'Crawford County': 0, 'Le Flore County': 0, 'Pulaski County': 0, 'Faulkner County': 0,
                   'Independence County': 0, 'Scott County': 0, 'Logan County': 0, 'Arkansas County': 0,
                   'Clark County': 0, 'Pike County': 0, 'Saline County': 0, 'Dekalb County': 0, 'Berrien County': 0,
                   'Cook County': 0, 'Fulton County': 0, 'Carroll County': 0, 'Cobb County': 0, 'Spalding County': 0,
                   'Lamar County': 0, 'Tift County': 0, 'Oconee County': 0, 'Clarke County': 0, 'Atkinson County': 0,
                   'Putnam County': 0, 'Greene County': 0, 'Randolph County': 0, 'Lowndes County': 0,
                   'Terrell County': 0, 'Webster County': 0, 'Hall County': 0, 'Clayton County': 0,
                   'Buchanan County': 0, 'Linn County': 0, 'Black Hawk County': 0, 'Mahaska County': 0,
                   'Emmet County': 0, 'Marshall County': 0, 'Louisa County': 0, 'Bremer County': 0, 'Johnson County': 0,
                   'Jasper County': 0, 'Polk County': 0, 'Warren County': 0, 'Muscatine County': 0,
                   'Cherokee County': 0, 'Plymouth County': 0, 'Woodbury County': 0, 'Dubuque County': 0,
                   'Jo Daviess County': 0, 'Lake County': 0, 'McHenry County': 0, 'Clinton County': 0,
                   'Winnebago County': 0, 'Ogle County': 0, 'Morgan County': 0, 'Kenosha County': 0, 'Dupage County': 0,
                   'Will County': 0, 'McLean County': 0, 'Kane County': 0, 'Peoria County': 0, 'Hendricks County': 0,
                   'Wells County': 0, 'Dubois County': 0, 'Warrick County': 0, 'Wayne County': 0, 'Fayette County': 0,
                   'Henry County': 0, 'Tipton County': 0, 'Wabash County': 0, 'Allen County': 0, 'St Joseph County': 0,
                   'LaPorte County': 0, 'Delaware County': 0, 'Hamilton County': 0, 'Ellis County': 0,
                   'Trego County': 0, 'Meade County': 0, 'Reno County': 0, 'Neosho County': 0, 'Labette County': 0,
                   'Riley County': 0, 'Sedgwick County': 0, 'Sherman County': 0, 'Graham County': 0,
                   'Shawnee County': 0, 'Jackson County': 0, 'Barton County': 0, 'Douglas County': 0,
                   'Mercer County': 0, 'Boyle County': 0, 'Garrard County': 0, 'Caldwell County': 0, 'Mason County': 0,
                   'Brown County': 0, 'Edmonson County': 0, 'Menifee County': 0, 'McCreary County': 0, 'Bath County': 0,
                   'Whitley County': 0, 'Knox County': 0, 'Laurel County': 0, 'Lincoln County': 0,
                   'Christian County': 0, 'Woodford County': 0, 'Jessamine County': 0, 'Bourbon County': 0,
                   'Calloway County': 0, 'Rowan County': 0, 'Clay County': 0, 'St Francois County': 0,
                   'Ste. Genevieve County': 0, 'Stoddard County': 0, 'Audrain County': 0, 'Pettis County': 0,
                   'St Louis County': 0, 'Platte County': 0, 'Dunklin County': 0, 'New Madrid County': 0,
                   'Harrison County': 0, 'Forrest County': 0, 'Rankin County': 0, 'Noxubee County': 0,
                   'George County': 0, 'Prentiss County': 0, 'Simpson County': 0, 'Copiah County': 0,
                   'Winston County': 0, 'Lafayette County': 0, 'Amite County': 0, 'Hinds County': 0, 'Hooker County': 0,
                   'Hayes County': 0, 'Custer County': 0, 'Otoe County': 0, 'Howard County': 0,
                   'Pottawattamie County': 0, 'Lancaster County': 0, 'Morrill County': 0, 'Buffalo County': 0,
                   'Cass County': 0, 'Thurston County': 0, 'Cuming County': 0, 'Saunders County': 0, 'Furnas County': 0,
                   'Harlan County': 0, 'Holt County': 0, 'Phelps County': 0, 'Keith County': 0,
                   'Scotts Bluff County': 0, 'Cuyahoga County': 0, 'Gallia County': 0, 'Richland County': 0,
                   'Ashland County': 0, 'Coshocton County': 0, 'Holmes County': 0, 'Vinton County': 0,
                   'Athens County': 0, 'Meigs County': 0, 'Darke County': 0, 'Trumbull County': 0, 'Mahoning County': 0,
                   'Seneca County': 0, 'Hancock County': 0, 'Wood County': 0, 'Lucas County': 0, 'Pickaway County': 0,
                   'Ross County': 0, 'Scioto County': 0, 'Franklin County': 0, 'Portage County': 0, 'Lorain County': 0,
                   'Comanche County': 0, 'Cleveland County': 0, 'Choctaw County': 0, 'Red River County': 0,
                   'Tulsa County': 0, 'Osage County': 0, 'Okmulgee County': 0, 'Pottawatomie County': 0,
                   'Canadian County': 0, 'Caddo County': 0, 'Adair County': 0, 'Oklahoma County': 0,
                   'McClain County': 0, 'Pittsburg County': 0, 'Latimer County': 0, 'Creek County': 0,
                   'Cocke County': 0, 'Davidson County': 0, 'Anderson County': 0, 'Hawkins County': 0, 'Dyer County': 0,
                   'Sullivan County': 0, 'Blount County': 0, 'Loudon County': 0, 'Carter County': 0, 'Unicoi County': 0,
                   'Wilson County': 0, 'Macon County': 0, 'Monroe County': 0, 'Obion County': 0, 'Kaufman County': 0,
                   'Harris County': 0, 'Van Zandt County': 0, 'Fort Bend County': 0, 'Nueces County': 0,
                   'Bexar County': 0, 'Guadalupe County': 0, 'Tarrant County': 0, 'Travis County': 0,
                   'Denton County': 0, 'Grayson County': 0},
        "zip": {'36028': 0, '36421': 0, '36038': 0, '36474': 0, '36109': 0, '36115': 0, '36110': 0, '36107': 0,
                '36114': 0, '36323': 0, '36117': 0, '35218': 0, '35234': 0, '35203': 0, '35211': 0, '35758': 0,
                '35020': 0, '35205': 0, '35208': 0, '35228': 0, '35204': 0, '35233': 0, '35005': 0, '35214': 0,
                '36703': 0, '36701': 0, '36091': 0, '36051': 0, '35046': 0, '35045': 0, '36067': 0, '36006': 0,
                '36037': 0, '35750': 0, '35761': 0, '35215': 0, '35217': 0, '35904': 0, '35954': 0, '35956': 0,
                '35952': 0, '35972': 0, '36613': 0, '36618': 0, '36575': 0, '36608': 0, '36582': 0, '36523': 0,
                '36544': 0, '36509': 0, '35040': 0, '35078': 0, '35178': 0, '36551': 0, '36567': 0, '32577': 0,
                '36435': 0, '36092': 0, '36093': 0, '36801': 0, '36830': 0, '36849': 0, '36769': 0, '36751': 0,
                '36695': 0, '36350': 0, '36360': 0, '36005': 0, '71854': 0, '75501': 0, '75503': 0, '72365': 0,
                '72386': 0, '72364': 0, '72668': 0, '72687': 0, '65733': 0, '65761': 0, '72644': 0, '72742': 0,
                '72740': 0, '72764': 0, '72762': 0, '72745': 0, '72765': 0, '72128': 0, '72104': 0, '72727': 0,
                '72701': 0, '72084': 0, '72773': 0, '72012': 0, '72007': 0, '72136': 0, '72045': 0, '72176': 0,
                '72152': 0, '71730': 0, '72153': 0, '72031': 0, '72051': 0, '71639': 0, '72110': 0, '72802': 0,
                '72801': 0, '72655': 0, '72683': 0, '72640': 0, '72641': 0, '72923': 0, '72916': 0, '72956': 0,
                '72903': 0, '72904': 0, '72908': 0, '72905': 0, '72901': 0, '74901': 0, '74902': 0, '72023': 0,
                '72501': 0, '72173': 0, '72944': 0, '72940': 0, '72927': 0, '72936': 0, '72042': 0, '71921': 0,
                '71943': 0, '72022': 0, '72011': 0, '72019': 0, '72015': 0, '72002': 0, '30035': 0, '30088': 0,
                '30038': 0, '30083': 0, '30034': 0, '30294': 0, '30032': 0, '30030': 0, '30317': 0, '30002': 0,
                '30316': 0, '31637': 0, '31647': 0, '30344': 0, '30116': 0, '30117': 0, '30076': 0, '30350': 0,
                '30338': 0, '30075': 0, '30328': 0, '30342': 0, '30068': 0, '30062': 0, '30067': 0, '30346': 0,
                '30224': 0, '30223': 0, '30257': 0, '30295': 0, '30292': 0, '31794': 0, '31749': 0, '30677': 0,
                '30605': 0, '30606': 0, '30621': 0, '30622': 0, '31650': 0, '31642': 0, '31024': 0, '30642': 0,
                '30678': 0, '39840': 0, '31636': 0, '31601': 0, '31606': 0, '39842': 0, '39877': 0, '31824': 0,
                '30507': 0, '30542': 0, '39886': 0, '30273': 0, '30260': 0, '30236': 0, '30297': 0, '30238': 0,
                '30288': 0, '30274': 0, '30349': 0, '30296': 0, '50644': 0, '50682': 0, '52329': 0, '50139': 0,
                '50062': 0, '50057': 0, '50163': 0, '50619': 0, '50636': 0, '52302': 0, '52328': 0, '52202': 0,
                '52233': 0, '52402': 0, '52411': 0, '52341': 0, '52213': 0, '52324': 0, '52332': 0, '50222': 0,
                '50149': 0, '50273': 0, '52047': 0, '52157': 0, '52159': 0, '50703': 0, '50702': 0, '50701': 0,
                '50613': 0, '52577': 0, '52595': 0, '52534': 0, '52404': 0, '50207': 0, '51334': 0, '51344': 0,
                '50158': 0, '52738': 0, '52737': 0, '50514': 0, '50629': 0, '50626': 0, '50668': 0, '50622': 0,
                '50676': 0, '52240': 0, '52245': 0, '52333': 0, '52358': 0, '52246': 0, '52242': 0, '52241': 0,
                '50169': 0, '50054': 0, '50009': 0, '50035': 0, '50228': 0, '50677': 0, '50047': 0, '50211': 0,
                '52776': 0, '52766': 0, '50320': 0, '52755': 0, '52739': 0, '51012': 0, '51024': 0, '51038': 0,
                '51108': 0, '51031': 0, '52001': 0, '52003': 0, '61025': 0, '52002': 0, '52068': 0, '60051': 0,
                '60041': 0, '60050': 0, '60012': 0, '60042': 0, '62231': 0, '62801': 0, '62252': 0, '62882': 0,
                '62250': 0, '61109': 0, '61052': 0, '61020': 0, '62650': 0, '60002': 0, '53104': 0, '61016': 0,
                '53179': 0, '60046': 0, '60081': 0, '60517': 0, '60440': 0, '60565': 0, '60540': 0, '60490': 0,
                '61705': 0, '61704': 0, '61761': 0, '61701': 0, '60192': 0, '60120': 0, '60118': 0, '60110': 0,
                '60123': 0, '60124': 0, '61772': 0, '60189': 0, '60532': 0, '60563': 0, '60010': 0, '60190': 0,
                '60555': 0, '60185': 0, '60564': 0, '60504': 0, '60502': 0, '61540': 0, '61537': 0, '61375': 0,
                '61603': 0, '61614': 0, '60411': 0, '60030': 0, '60073': 0, '61753': 0, '61776': 0, '61748': 0,
                '61744': 0, '61730': 0, '61726': 0, '60651': 0, '60623': 0, '60624': 0, '60647': 0, '60639': 0,
                '60804': 0, '60612': 0, '60632': 0, '60644': 0, '60638': 0, '60629': 0, '46231': 0, '46168': 0,
                '46122': 0, '46121': 0, '46714': 0, '46235': 0, '46219': 0, '46226': 0, '46239': 0, '46203': 0,
                '46218': 0, '46107': 0, '46201': 0, '47118': 0, '47175': 0, '47541': 0, '47523': 0, '47585': 0,
                '47637': 0, '47537': 0, '47620': 0, '46220': 0, '47542': 0, '47619': 0, '47357': 0, '47327': 0,
                '47335': 0, '47374': 0, '47331': 0, '47387': 0, '47346': 0, '47302': 0, '47362': 0, '47660': 0,
                '47330': 0, '47370': 0, '46076': 0, '46072': 0, '46941': 0, '46992': 0, '46990': 0, '46819': 0,
                '46809': 0, '46545': 0, '46544': 0, '46615': 0, '46635': 0, '46614': 0, '46617': 0, '46613': 0,
                '46637': 0, '46601': 0, '46625': 0, '46626': 0, '46307': 0, '46375': 0, '46373': 0, '46556': 0,
                '46319': 0, '46311': 0, '46321': 0, '60417': 0, '46241': 0, '46224': 0, '46214': 0, '46234': 0,
                '46123': 0, '46340': 0, '46382': 0, '46390': 0, '46532': 0, '47396': 0, '47334': 0, '46001': 0,
                '46012': 0, '46011': 0, '46322': 0, '46250': 0, '46038': 0, '46256': 0, '47342': 0, '67637': 0,
                '67656': 0, '67301': 0, '67869': 0, '67502': 0, '67501': 0, '67505': 0, '66740': 0, '66776': 0,
                '67357': 0, '66733': 0, '67341': 0, '66720': 0, '66503': 0, '67337': 0, '67220': 0, '67208': 0,
                '67218': 0, '67211': 0, '67214': 0, '67219': 0, '67202': 0, '67213': 0, '67203': 0, '67204': 0,
                '67217': 0, '67209': 0, '67212': 0, '67205': 0, '67260': 0, '67735': 0, '67642': 0, '66763': 0,
                '66735': 0, '66712': 0, '66743': 0, '66756': 0, '66762': 0, '66711': 0, '67042': 0, '67002': 0,
                '67230': 0, '66533': 0, '66418': 0, '66536': 0, '66422': 0, '66605': 0, '66542': 0, '66607': 0,
                '67530': 0, '66046': 0, '66044': 0, '66045': 0, '66049': 0, '66047': 0, '66006': 0, '66085': 0,
                '66209': 0, '66224': 0, '66223': 0, '66213': 0, '66210': 0, '66221': 0, '66062': 0, '40330': 0,
                '40422': 0, '40444': 0, '42445': 0, '42459': 0, '42437': 0, '42411': 0, '42164': 0, '42122': 0,
                '41056': 0, '45101': 0, '41096': 0, '45144': 0, '45154': 0, '42101': 0, '42159': 0, '42171': 0,
                '42503': 0, '42544': 0, '42210': 0, '42633': 0, '40229': 0, '40219': 0, '40316': 0, '40322': 0,
                '42647': 0, '40291': 0, '40371': 0, '40346': 0, '40701': 0, '40771': 0, '40759': 0, '40387': 0,
                '40358': 0, '40734': 0, '42501': 0, '40489': 0, '40442': 0, '42567': 0, '42240': 0, '42217': 0,
                '40510': 0, '40513': 0, '40383': 0, '40353': 0, '40361': 0, '40391': 0, '40509': 0, '40516': 0,
                '40505': 0, '40515': 0, '40511': 0, '40517': 0, '40502': 0, '40508': 0, '40507': 0, '40503': 0,
                '40504': 0, '40033': 0, '40069': 0, '42718': 0, '42036': 0, '42020': 0, '40351': 0, '40313': 0,
                '40319': 0, '37042': 0, '42262': 0, '42286': 0, '42223': 0, '37040': 0, '40526': 0, '64056': 0,
                '64058': 0, '64057': 0, '64050': 0, '64079': 0, '64054': 0, '64055': 0, '64052': 0, '64053': 0,
                '64126': 0, '64129': 0, '64125': 0, '64120': 0, '64123': 0, '64127': 0, '64124': 0, '64128': 0,
                '64130': 0, '64117': 0, '64109': 0, '64116': 0, '64108': 0, '64106': 0, '64105': 0, '63028': 0,
                '63020': 0, '63087': 0, '63036': 0, '63628': 0, '63841': 0, '64673': 0, '64661': 0, '65265': 0,
                '65301': 0, '65075': 0, '65486': 0, '65459': 0, '64138': 0, '65201': 0, '65202': 0, '64133': 0,
                '65203': 0, '63033': 0, '63136': 0, '63034': 0, '63135': 0, '63121': 0, '63031': 0, '63134': 0,
                '63042': 0, '63114': 0, '63140': 0, '63074': 0, '64158': 0, '64068': 0, '64157': 0, '64119': 0,
                '64156': 0, '64155': 0, '64118': 0, '63145': 0, '63044': 0, '64151': 0, '64150': 0, '64154': 0,
                '64152': 0, '64153': 0, '65256': 0, '65284': 0, '64651': 0, '63857': 0, '63848': 0, '63837': 0,
                '39564': 0, '39532': 0, '39176': 0, '39192': 0, '38967': 0, '39530': 0, '38917': 0, '39455': 0,
                '39475': 0, '391765644': 0, '39047': 0, '39042': 0, '39232': 0, '39208': 0, '39341': 0, '39117': 0,
                '39145': 0, '39702': 0, '39766': 0, '39743': 0, '39739': 0, '39452': 0, '38824': 0, '38849': 0,
                '39507': 0, '39501': 0, '39503': 0, '39082': 0, '39059': 0, '39078': 0, '39560': 0, '39191': 0,
                '39601': 0, '39465': 0, '39401': 0, '39339': 0, '39563': 0, '39581': 0, '39567': 0, '39553': 0,
                '39562': 0, '38673': 0, '38655': 0, '39666': 0, '39629': 0, '39664': 0, '39647': 0, '39346': 0,
                '39216': 0, '39202': 0, '39201': 0, '39213': 0, '39203': 0, '39206': 0, '39204': 0, '39209': 0,
                '39217': 0, '69152': 0, '68666': 0, '68651': 0, '69032': 0, '68822': 0, '68329': 0, '68824': 0,
                '68324': 0, '51510': 0, '68108': 0, '68110': 0, '68102': 0, '51501': 0, '68107': 0, '68112': 0,
                '68111': 0, '68105': 0, '68131': 0, '68178': 0, '68106': 0, '68104': 0, '68132': 0, '68117': 0,
                '68152': 0, '68114': 0, '68122': 0, '68134': 0, '68124': 0, '68127': 0, '68182': 0, '68144': 0,
                '68154': 0, '68164': 0, '68521': 0, '68524': 0, '69336': 0, '69334': 0, '68847': 0, '68845': 0,
                '68531': 0, '68849': 0, '68048': 0, '68352': 0, '68869': 0, '68047': 0, '68004': 0, '68791': 0,
                '68716': 0, '68840': 0, '68801': 0, '68803': 0, '68810': 0, '68505': 0, '68507': 0, '68527': 0,
                '68506': 0, '68510': 0, '68520': 0, '68504': 0, '68041': 0, '68073': 0, '68948': 0, '68922': 0,
                '69022': 0, '68966': 0, '68977': 0, '68967': 0, '68763': 0, '68746': 0, '68787': 0, '68949': 0,
                '69165': 0, '69155': 0, '69361': 0, '69341': 0, '45429': 0, '45459': 0, '45419': 0, '45439': 0,
                '45449': 0, '45409': 0, '44107': 0, '25550': 0, '45631': 0, '44843': 0, '44813': 0, '44903': 0,
                '44864': 0, '45011': 0, '45044': 0, '45050': 0, '44094': 0, '44095': 0, '44092': 0, '44143': 0,
                '44132': 0, '44124': 0, '44117': 0, '44123': 0, '44119': 0, '44110': 0, '43812': 0, '44637': 0,
                '43844': 0, '44112': 0, '44654': 0, '43805': 0, '45692': 0, '45634': 0, '45640': 0, '45710': 0,
                '45769': 0, '45701': 0, '45695': 0, '45651': 0, '45328': 0, '45331': 0, '45308': 0, '16159': 0,
                '44425': 0, '44505': 0, '44436': 0, '44853': 0, '44830': 0, '44802': 0, '44438': 0, '44883': 0,
                '43402': 0, '43569': 0, '43522': 0, '43565': 0, '43551': 0, '43164': 0, '43115': 0, '45629': 0,
                '43219': 0, '43211': 0, '44231': 0, '44288': 0, '44234': 0, '44255': 0, '44055': 0, '44035': 0,
                '44266': 0, '44052': 0, '44053': 0, '44001': 0, '44089': 0, '73501': 0, '73507': 0, '73505': 0,
                '73160': 0, '73135': 0, '74761': 0, '74735': 0, '75436': 0, '74756': 0, '74764': 0, '74112': 0,
                '74115': 0, '74104': 0, '74110': 0, '74114': 0, '74120': 0, '74106': 0, '74119': 0, '74103': 0,
                '74127': 0, '74186': 0, '74464': 0, '74451': 0, '74465': 0, '74446': 0, '74445': 0, '74447': 0,
                '74437': 0, '74421': 0, '74804': 0, '74801': 0, '74851': 0, '73843': 0, '73521': 0, '73556': 0,
                '73560': 0, '73523': 0, '73526': 0, '73047': 0, '73053': 0, '73048': 0, '74931': 0, '74471': 0,
                '73003': 0, '73012': 0, '73025': 0, '73045': 0, '74857': 0, '74012': 0, '74011': 0, '73080': 0,
                '73170': 0, '73139': 0, '73159': 0, '73173': 0, '74501': 0, '74547': 0, '74522': 0, '74554': 0,
                '74502': 0, '73007': 0, '73034': 0, '73049': 0, '73013': 0, '73131': 0, '74578': 0, '74545': 0,
                '74546': 0, '74576': 0, '74047': 0, '74041': 0, '74066': 0, '74136': 0, '74073': 0, '74126': 0,
                '74006': 0, '74003': 0, '74029': 0, '37821': 0, '37843': 0, '38341': 0, '38320': 0, '37013': 0,
                '37211': 0, '37218': 0, '37027': 0, '37880': 0, '37322': 0, '37214': 0, '37210': 0, '37216': 0,
                '37217': 0, '37206': 0, '37115': 0, '37207': 0, '37754': 0, '37849': 0, '37931': 0, '37716': 0,
                '37830': 0, '37710': 0, '37110': 0, '37357': 0, '37642': 0, '37873': 0, '24251': 0, '37857': 0,
                '24244': 0, '37731': 0, '37806': 0, '37924': 0, '37914': 0, '37871': 0, '37721': 0, '37918': 0,
                '37917': 0, '38024': 0, '37665': 0, '37660': 0, '37803': 0, '37801': 0, '37737': 0, '37742': 0,
                '37643': 0, '37601': 0, '37694': 0, '37692': 0, '37604': 0, '37605': 0, '37614': 0, '37215': 0,
                '37205': 0, '37421': 0, '37363': 0, '37416': 0, '37087': 0, '37090': 0, '37122': 0, '38506': 0,
                '37083': 0, '37150': 0, '42140': 0, '42133': 0, '38260': 0, '38240': 0, '38253': 0, '38232': 0,
                '75126': 0, '77493': 0, '77449': 0, '75169': 0, '75103': 0, '77407': 0, '77083': 0, '77406': 0,
                '11870': 0, '77450': 0, '78412': 0, '78414': 0, '78411': 0, '78413': 0, '90058': 0, '78404': 0,
                '78415': 0, '78401': 0, '78405': 0, '78416': 0, '78417': 0, '78245': 0, '78148': 0, '78154': 0,
                '78109': 0, '78233': 0, '78239': 0, '78266': 0, '78247': 0, '75052': 0, '75051': 0, '75050': 0,
                '76011': 0, '76010': 0, '76105': 0, '76006': 0, '76014': 0, '76018': 0, '76012': 0, '76013': 0,
                '76015': 0, '76040': 0, '76017': 0, '76053': 0, '76016': 0, '76120': 0, '76118': 0, '76112': 0,
                '78744': 0, '78704': 0, '78774': 0, '78745': 0, '78747': 0, '78748': 0, '78746': 0, '78753': 0,
                '78652': 0, '78735': 0, '78749': 0, '76258': 0, '76227': 0, '76266': 0, '75241': 0, '75216': 0,
                '75134': 0, '75204': 0, '75203': 0, '75224': 0, '75232': 0, '75208': 0, '75115': 0, '75237': 0,
                '75233': 0, '75236': 0, '75116': 0, '75137': 0, '76180': 0, '76182': 0, '76148': 0, '76137': 0,
                '76117': 0, '76244': 0, '78209': 0, '78217': 0, '78216': 0, '78213': 0, '78246': 0}
    }
    exposure_dictionary = copy.deepcopy(loss_dictionary)
    premium_dictionary = copy.deepcopy(loss_dictionary)

    random.shuffle(states)

    exposures = []

    generate_start = time.time()

    for loss_state in states:
        num_states = randint(1700, 5000)
        curr_props = db["sample houses random"].aggregate([
            {
                "$match": {
                    "state": loss_state
                }
            },
            {
                "$sample": {
                    "size": num_states
                }
            }
        ])

        for curr_prop in curr_props:
            if exposure_dictionary['total'] > (total_agg_limit - 15000) or exposure_dictionary['state'][curr_prop['state']] > (state_agg_limit - 15000):
                break

            curr_state = curr_prop['state']
            curr_county = curr_prop['county']
            curr_zip = curr_prop['zip_code']
            curr_limit = curr_prop['limit']

            premium = limit_to_premium(curr_limit)

            exposure_dictionary['total'] += curr_limit
            exposure_dictionary['state'][curr_state] += curr_limit
            exposure_dictionary['county'][curr_county] += curr_limit
            exposure_dictionary['zip'][curr_zip] += curr_limit
            premium_dictionary['total'] += premium
            premium_dictionary['state'][curr_state] += premium
            premium_dictionary['county'][curr_county] += premium
            premium_dictionary['zip'][curr_zip] += premium

            exposures.append(curr_prop)

        if exposure_dictionary['total'] > (total_agg_limit - 15000):
            break
    generate_end = time.time()

    print("Generate Time", generate_end - generate_start)

    insert_start = time.time()

    db[f"sample houses select{test}"].delete_many({})
    db[f"sample houses select{test}"].insert_many(exposures)

    insert_end = time.time()

    print("Insert Time", insert_end - insert_start)

    upload_exposure_start = time.time()

    exposure_list = [test]
    premium_list = [test]

    for key, value in exposure_dictionary.items():
        if key == 'total':
            exposure_list.append(value)
            premium_list.append(premium_dictionary[key])
        else:
            for key2, value2 in value.items():
                exposure_list.append(value2)
                premium_list.append(premium_dictionary[key][key2])

    upload_csv(exposure_list, "../files/output/exposures.csv")
    upload_csv(premium_list, "../files/output/premium.csv")

    upload_exposure_end = time.time()

    print("Upload Exposures", upload_exposure_end - upload_exposure_start)

    generate_torn_start = time.time()

    num_tornadoes = 0
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

    generate_torn_end = time.time()

    print("Make Tornadoes", generate_torn_end - generate_torn_start)

    search_torn_start = time.time()

    for severity, torn_poly in multi_tornado_dict.items():
        if severity not in avg_payout_dict:
            continue
        if len(torn_poly['geometry']['coordinates']) > 0:
            damaged_homes = db[f"sample houses select{test}"].find({
                "location": {
                    "$geoWithin": {
                        "$geometry": {
                            "type": torn_poly['geometry']['type'],
                            "coordinates": torn_poly['geometry']['coordinates']
                        }
                    }
                }
            })

            payout = avg_payout_dict[severity]

            for damaged_home in damaged_homes:
                loss_dictionary['total'] += payout
                loss_dictionary['state'][damaged_home['state']] += payout
                loss_dictionary['county'][damaged_home['county']] += payout
                loss_dictionary['zip'][damaged_home['zip_code']] += payout

    search_torn_end = time.time()

    print("Search Tornadoes", search_torn_end - search_torn_start)

    loss_list = [test]
    nlr_list = [test]

    for key, value in loss_dictionary.items():
        if key == 'total':
            loss_list.append(value)
            nlr_list.append(generate_loss_ratio(premium_dictionary[key], value))
        else:
            for key2, value2 in value.items():
                loss_list.append(value2)
                nlr_list.append(generate_loss_ratio(premium_dictionary[key][key2], value2))

    upload_csv(loss_list, "../files/output/losses.csv")
    upload_csv(nlr_list, "../files/output/nlr.csv")

    return test


if __name__ == '__main__':
    sims = list(range(50000))

    with Pool(100) as p:
        print(p.map(run_simulation, sims))


