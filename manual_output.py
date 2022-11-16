import json
import csv
import requests
import logging
import glob
import os
from dotenv import load_dotenv


def get_authorization_token():
    load_dotenv()
    authorization_token = os.environ.get('authorization_token')

    return authorization_token


def filename():
    csv_file = glob.glob('*.csv')[0]
    return csv_file


def csv_to_json():
    jsonArray = []
    inputs = {}
    webServiceInput0 = {}

    manual_input_file = filename()

    # Encoding is set that option to remove Byte Order Mark in id key
    with open(manual_input_file, encoding='utf-8-sig') as csvfile:
        csvReader = csv.DictReader(csvfile)
        for row in csvReader:
            jsonArray.append(row)

    # Remove the flag bought column
    for i in jsonArray:
            i.pop("flag_bought", None)

    webServiceInput0['WebServiceInput0'] = jsonArray
    inputs['Inputs'] = webServiceInput0
    inputs['GlobalParameters'] = {}

    jsonString = json.dumps(inputs, indent=4)

    return jsonString


def calculate_score(inputData):
    bearer_token = get_authorization_token()
    url = "https://mymagicnumberi59jz7.southeastasia.cloudapp.azure.com:443/api/v1/service/crystalball-01-lg-r01/score"
    headers = {
        'Content-Type': 'application/json',
        'Authorization':f'{bearer_token}'
        }
    
    # To pass the JSON payload to endpoint for probability score
    response = requests.request("POST", url, headers=headers, data=inputData)
    json_data = json.loads(response.text)

    logging.info("Probability score is calculated")
    logging.info(f"Prediction result: {json_data}")

    return json_data


if __name__ == "__main__":
    logging.basicConfig(level = logging.INFO)

    try:
        ml_payload = csv_to_json()
        calculate_score(ml_payload)
        logging.info("Process is successful")
    except Exception as e:
        logging.info(f"{e}")