
import azure.cosmos.cosmos_client as cosmos_client
import timeit
import pandas as pd
from datetime import datetime
import json
import uuid
from dotenv import dotenv_values
from tqdm import tqdm

date_time = datetime.now()


config = dotenv_values(".env")

HOST = config['cosmosdb_host']
MASTER_KEY = config['cosmosdb_master_key']
DATABASE_ID = config['cosmosdb_database_id']


def create_item(container, item):
    container.create_item(body=item)


def readCsvBcSendCosmosDB(company, endpoint, erp, file_name, container):
    df = pd.read_excel("files/from_bc/" + file_name)
    df["DW_EVI_BU"] = company
    df["DW_ERP_System"] = erp
    df["DW_Timestamp"] = date_time
    df["DW_ERP_Source_Table"] = endpoint
    # df["id"] = company + "_" + df["No"]
    print(df)

    # pd.read_csv("files/from_bc/" + company + "_" +
    #             endpoint + "_" + date_time.strftime("%m%d%y") + '.csv.gz', compression="gzip")
    # print(df)
    # df = df.sample(n=20)
    # print(df)
    # getColumns(df)
    data = df.to_json(orient='records')
    # # print(type(data))
    data_dict = json.loads(data)
    for item in tqdm(data_dict):
        #     # Assign id to the item
        item['id'] = str(uuid.uuid4())
        create_item(container, item)


def main():
    client = cosmos_client.CosmosClient(HOST, {'masterKey': MASTER_KEY})
    db = client.get_database_client(DATABASE_ID)
    container = db.get_container_client("Items")

    endpoint = "VINITEM"
    company = "PAC/PLS"
    erp = "s2k"
    file_name = "vinitem.xlsx"
    readCsvBcSendCosmosDB(company, endpoint, erp, file_name, container)


if __name__ == '__main__':
    start = timeit.default_timer()
    main()
    end = timeit.default_timer()
    print("Duration: ", end-start, "secs")
    print("Duration: ", (end-start)/60, "mins")
