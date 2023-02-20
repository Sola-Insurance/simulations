from functions.connect_to_db import get_database
import gspread
import csv

db = get_database()

gc = gspread.service_account(filename='../files/credentials.json')

sh = gc.open_by_key('1INGKUPbuG252SgYVGpTHGXuEE_41V1evgaTOIZ0bix4')
loss_ws = sh.worksheet('New Losses')

sample_houses = db["sample houses random"].find({})

states = ['AL', 'AR', 'GA', 'IA', 'IL', 'IN', 'KS', 'KY', 'MO', 'MS', 'NE', 'OH', 'OK', 'TN', 'TX']

state_dict = {}
county_dict = {}
zip_dict = {}

for sample_house in sample_houses:
    if sample_house['state'] not in state_dict:
        state_dict[sample_house['state']] = 0
    if sample_house['county'] not in county_dict:
        county_dict[sample_house['county']] = 0
    if sample_house['zip_code'] not in zip_dict:
        zip_dict[sample_house['zip_code']] = 0

# print("States", state_dict)
# print("Counties", county_dict)
# print("Zips", zip_dict)

headers = ["Total"]

for state in state_dict:
    headers.append(state)

for county in county_dict:
    headers.append(county)

for zip_code in zip_dict:
    headers.append(zip_code)

with open('../files/output/exposures.csv', 'a') as f:
    writer = csv.writer(f)
    writer.writerow(headers)

