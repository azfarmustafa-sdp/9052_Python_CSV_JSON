# ML Data Extraction

#### This project is to use a manual input CSV file or call a stored procedure from DMP MySQL database and pass the output data to Azure ML endpoint to generate prediction label and probability score.

## Prerequisites

Before begin, ensure you have met the following requirements:
* Installed python version 3.8 and above.
* Have downloaded SSL certificate in the project folder for MySQL database connection.
* Setup the .env file for credentials.
* Create python virtual environment and setup the python executor path.

## Installation
Execute `requirements.txt` to install the needed packages in the virtual environment.

## Execution Using Manual Output
1. Put the CSV file in the project root folder.
2. Make sure there is only one CSV file in the project root folder.
2. Then, run the code in `manual_output.py` to get the prediction result. 

## Execution Using Stored Procedure
1. Run the code in `stored_proc_output.py` to get the prediction result.