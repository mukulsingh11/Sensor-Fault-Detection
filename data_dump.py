import pymongo
import pandas as pd
import json
from sensor.config import mongo_client

DATE_FILE_PATH = (r"notebook\aps_failure_training_set1.csv")
DATABASE_NAME = "aps"
COLLECTION_NAME = "sensor"

if __name__ == "__main__":
    df = pd.read_csv(DATE_FILE_PATH) 
    print(f"Rows and Columns: {df.shape}")

    #Convert dataframe to json so that we can dump these record in mongodb
    df.reset_index(drop=True, inplace=True)

    json_record = list(json.loads(df.T.to_json()).values())
    print(json_record[0])

    #Insert converted json record to mongodb
    mongo_client[DATABASE_NAME][COLLECTION_NAME].insert_many(json_record)



