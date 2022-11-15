import pymysql
import os
import json
import requests
import logging
from dotenv import load_dotenv
from pymysql.constants import FIELD_TYPE
from pymysql.converters import conversions


def get_database_connection():
    load_dotenv()
    hostName = os.environ.get('hostName')
    userName = os.environ.get('userName')
    password = os.environ.get('password')
    databaseName = os.environ.get('databaseName')

    return hostName, userName, password, databaseName


def get_authorization_token():
    load_dotenv()
    authorization_token = os.environ.get('authorization_token')

    return authorization_token


def execute_stored_proc():
    # To initiate empty dictionary to store stored proc result
    inputs = {}
    webServiceInput0 = {}

    # To remove the decimal and datetime keyword in stored proc output
    conv = conversions.copy()
    conv[FIELD_TYPE.NEWDECIMAL] = float
    conv[FIELD_TYPE.DATETIME] = str

    hostName, userName, password, databaseName = get_database_connection()

    databaseConnection = pymysql.connect(host=hostName,
                                     user=userName,
                                     password=password, 
                                     db=databaseName,
                                     # SSL certificate value should be according to the absolute/relative path
                                     ssl_ca='BaltimoreCyberTrustRoot.crt.pem',
                                     charset="utf8",
                                     cursorclass=pymysql.cursors.DictCursor,
                                     conv=conv)
                            
    try:
        cursorObject = databaseConnection.cursor()
        cursorObject.execute("call digitalmarketuatclone.crystalballrun()")
        result = cursorObject.fetchall()

        # To remove keyword flag_bought from the stored proc output
        for i in result:
            i.pop("flag_bought", None)
        
        # To store stored proc result in JSON 
        webServiceInput0['WebServiceInput0'] = result
        inputs['Inputs'] = webServiceInput0
        inputs['GlobalParameters'] = {}
        payload = json.dumps(inputs)

        logging.info("Stored proc executed successfully")

        return payload

    except Exception as e:
        logging.error(f"Exception occured: {e}")

    finally:
        databaseConnection.close()


def calculate_score(inputData):
    bearer_token = get_authorization_token()
    url = "https://mymagicnumberi59jz7.southeastasia.cloudapp.azure.com:443/api/v1/service/crystalball-01-lg-r01/score"
    headers = {
        'Content-Type': 'application/json',
        'Authorization':f'{bearer_token}'
        }
    
    # To pass the JSON payload to endpoint for probability score
    response = requests.request("POST", url,headers=headers, data = inputData)
    json_data = json.loads(response.text)

    logging.info("Probability score is calculated")
    logging.info(f"Prediction result: {json_data}")

    return json_data


if __name__ == "__main__":
    logging.basicConfig(level = logging.INFO)

    try:
        ml_payload = execute_stored_proc()
        calculate_score(ml_payload)
        logging.info("Process is successful")
    except Exception as e:
        logging.info(f"{e}")