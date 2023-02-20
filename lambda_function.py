from functions.generate_sample_tornado import run_simulation
import json


def lambda_handler(event, context):
    run_simulation()

    return {
        "statusCode": 200,
        "body": json.dumps({"bro": "bro"})
    }