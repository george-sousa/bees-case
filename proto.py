# def pipedrive_func():
#    import json
#    import urllib.request as urllib2
#    import pandas as pd
#    import requests
#    import os
#    import sqlalchemy
#    # o token abaixo foi obtido a partir do usuario do LucianoMoura
#    token = "c5c7c46048fdf8fa3c167d0179a65edaf32f96f0"; 

#    headers = {
#    'cache-control': 'no-cache',
#    'content-type': 'application/json',
#    'x-ratelimit-limit': '20',
#    'x-ratelimit-remaining': '19',
#    'x-ratelimit-reset': '2'
#    }

#    entidades = ['pd_deal', 'pd_deal_fields', 'pd_stage']
#    for entidade in entidades:

#       pageBucket = 100
#       startPage = 0
#       more_itens = True

#       print("Carregando dados de [" + entidade + "]")
      
#       if os.path.exists("/opt/airflow/src/projetos/pipedrive/temp.csv"):
#          os.remove("/opt/airflow/src/projetos/pipedrive/temp.csv")
#       if os.path.exists("/opt/airflow/src/projetos/pipedrive/temp_opt.csv"):
#          os.remove("/opt/airflow/src/projetos/pipedrive/temp_opt.csv")
      
#       #endereço utilizado para busca de dados do Pipedrive via REST
#       if (entidade=='pd_deal'):
#          url = "https://unisoma.pipedrive.com/api/v1/deals?api_token=" + token
#       elif (entidade=='pd_deal_fields'):
#          url = "https://unisoma.pipedrive.com/api/v1/dealFields?api_token=" + token
#       elif (entidade=='pd_stage'):
#          url = "https://unisoma.pipedrive.com/api/v1/stages?api_token=" + token

#       while (more_itens):
      
#          #obtem do Pipedrive a lista de todos os registros e coloca em um data frame
#          response = requests.request("GET", url + "&limit={}&start={}".format(pageBucket,startPage), headers = headers)
#          responseJson = response.json()
#          print("Dados obtidos do Pipedrive: Bloco [" + str(startPage) + "] em [" + url + "]")

#          if (responseJson['success']==True):
#             # remove colunas indesejaveis...
#             df = pd.DataFrame(responseJson['data'])
            
#             if (entidade=='pd_deal'):
#                df.drop(columns=['next_activity_duration', 'next_activity_type', 'org_hidden', 
#                   'person_name', 'label', 'id', 'next_activity_note', 
#                   'next_activity_subject', 'cc_email', 'formatted_weighted_value', 'formatted_value',# 'renewal_type',
#                   'visible_to', 'weighted_value_currency', 'rotten_time', 'products_count', 'person_hidden', 
#                   'next_activity_id', 'next_activity_time', 'next_activity_date', 'last_activity_date', 
#                   'last_activity_id', 'deleted'], axis=1, inplace=True) #'person_id', 'creator_user_id', 'user_id', 'org_id',
#             elif (entidade=='pd_deal_fields'):
#                df.drop(columns=['id', 'order_nr','add_time','update_time', 'last_updated_by_user_id', 'active_flag', 'is_subfield',
#                   'edit_flag',# 'index_visible_flag',
#                   'details_visible_flag', 'add_visible_flag', 'important_flag', 'link',
#                   'mandatory_flag', 'bulk_edit_allowed', 'searchable_flag' ,'filtering_allowed', 'sortable_flag',
#                   'use_field'], axis=1, inplace=True)
#             elif (entidade=='pd_stage'):
#                df.drop(columns=['active_flag', 'order_nr','deal_probability','rotten_flag', 'rotten_days', 'add_time', 'update_time',
#                   'pipeline_name', 'pipeline_deal_probability'], axis=1, inplace=True)

#             # executa ajustes especificos para algumas entidades...
#             if (entidade=='pd_deal_fields'):
#                dfo = pd.DataFrame(df)
#                df.drop(columns=['options'], axis=1, inplace=True)
#                headerOps = True
#                for index, row in dfo.iterrows():
#                   for i, v in row.items():
#                      if i=='options' and isinstance(v, list):
#                         dfops = pd.DataFrame(v)
#                         try: 
#                             quantity = dfops['id'].size
#                         except:
#                             quantity = dfops.size # maybe this should be 0?

#                         keyColumn = [row['key']] * quantity#dfops['id'].size 
#                         dfops['key'] = keyColumn
#                         dfops.to_csv('/opt/airflow/src/projetos/pipedrive/temp_opt.csv', mode='a', sep=';', header=headerOps)
#                         headerOps = False
#             elif (entidade=='pd_deal'):
#                df = df.rename(columns={'312521374381ae05ea73c9db8cecf159132a6df0': 'origem_lead', 
#                                        '684ec5d0d2cb9a47388ca5f9cc1bf4c1b555f466': 'vigencia_contrato_cta_az',
#                                        '6be53f6d38bdd5b454735fed442f788cdb097ac6': 'cobranca_cta_az', 
#                                        'a9b16524685f4ab65ce3d275e9a8265c0b4b7357': 'esforco_estimado',
#                                        'e97fc33357a99474a01f1f65de76b3341494a9bc': 'contrato_cta_az',
#                                        'd3ff8fe59c00f182d88054b01d3c3cadf8827bcc': 'lead_type'
#                                        })

#             df.to_csv('/opt/airflow/src/projetos/pipedrive/temp.csv', mode='a', sep=';', header=(startPage==0))
      
#             # verifica se existem mais registros na proxima pagina e incrementa o inicio 
#             # da pagina para o proximo request
#             try:
#                adDf = pd.DataFrame(responseJson['additional_data'])
#                more_itens = adDf['pagination']['more_items_in_collection']
#                startPage = startPage + pageBucket
#             except KeyError:
#                startPage = 1
#                more_itens = False
#                pass

#       # salva dados do CSV no banco de dados
#       fields = []
#       if (entidade=='pd_deal'):
#          fields=['origem_lead','vigencia_contrato_cta_az','cobranca_cta_az','esforco_estimado','active','activities_count',
#                'add_time','close_time','currency','done_activities_count','contrato_cta_az','email_messages_count','expected_close_date',
#                'files_count','first_won_time','followers_count','last_incoming_mail_time','last_outgoing_mail_time','lost_reason',
#                'lost_time','notes_count','org_name','owner_name','participants_count','pipeline_id','probability','stage_change_time',
#                'stage_id','stage_order_nr','status','title','undone_activities_count','value','weighted_value','won_time', 'lead_type']
#       elif (entidade=='pd_deal_fields'):
#          fields = ['id','label','key']
#          persist_to_db('pd_deal_fields_option', "/opt/airflow/src/projetos/pipedrive/temp_opt.csv", fields)
#          fields = ['field_type','key','name']
#       elif (entidade=='pd_stage'):
#          fields = ['id','name','pipeline_id']

#       persist_to_db(entidade, "/opt/airflow/src/projetos/pipedrive/temp.csv", fields)
      
#    print("Carga de dados Pipedrive concluida.")
# import requests

# x = requests.get('https://api.openbrewerydb.org/v1/breweries')#, headers=headers)
# import pandas as pd
# df = pd.DataFrame.from_dict(x.json())
# print(df)
import requests
import json
# import boto3
import os
from datetime import datetime
from pathlib import Path
# from botocore.exceptions import ClientError

# Constants
API_URL = "https://api.openbrewerydb.org/v1/breweries"
PER_PAGE = 50  # max is 200 per the API
# S3_BUCKET = os.getenv("S3_BUCKET", "my-brewery-data-lake")
S3_BASE_PATH = "bronze"
BRONZE_PATH = "./data-lake/bronze/open-brewery-api"
MAX_PAGES = 1000  # safety limit

# s3 = boto3.client("s3")


# def fetch_brewery_page(page: int, per_page: int = PER_PAGE):
def fetch_brewery_page(page: int, per_page: int ):
    response = requests.get(API_URL, params={"page": page, "per_page": per_page})
    if response.status_code != 200:
        raise Exception(f"Failed to fetch page {page}: {response.status_code}")
    return response.json()


# def upload_to_s3(data: dict, key: str):
#     try:
#         s3.put_object(
#             Bucket=S3_BUCKET,
#             Key=key,
#             Body=json.dumps(data),
#             ContentType="application/json",
#         )
#         print(f"Uploaded to s3://{S3_BUCKET}/{key}")
#     except ClientError as e
#         print(f"Failed to upload to S3: {e}")
#         raise


def extract_to_bronze_s3(per_page: int, execution_date: str = None):
    """
    Main extraction function. Fetches all brewery pages and stores in S3.
    Parameters:
        execution_date (str): ISO format date (YYYY-MM-DD). Defaults to today.
    """
    number_of_breweries_found = 0
    all_breweries = []
    if not execution_date:
        execution_date = datetime.today().strftime("%Y-%m-%d")

    output_dir = Path(f"{BRONZE_PATH}/{execution_date}")
    output_dir.mkdir(parents=True, exist_ok=True)

    for page in range(1, MAX_PAGES + 1):
        # print(f"Fetching page {page}")
        breweries = fetch_brewery_page(page, per_page)

        if not breweries:
            # print(f"No data on page {page}. Stopping.")
            break

        # Handling overfetching, to reduce bandwidth use
        # elif len(breweries) < per_page:
        #     number_of_breweries_found += len(breweries)
        #     # print(breweries)
        #     # print(len(breweries))
        #     # print(f"Reached end of data at page {page}.")
        #     break

        number_of_breweries_found += len(breweries)
        all_breweries.extend(breweries)

        # key = f"{S3_BASE_PATH}/{execution_date}/page={page}.json"
        # key = f"{S3_BASE_PATH}/{execution_date}/page={page}.json"

        # Idempotency check: skip if file already exists
        # try:
        #     s3.head_object(Bucket=S3_BUCKET, Key=key)
        #     print(f"File already exists: {key} — skipping.")
        #     continue
        # except ClientError as e:
        #     if e.response['Error']['Code'] != '404':
        #         raise

            # Save to JSON file
        output_file = output_dir / "breweries.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(all_breweries, f, indent=2)

        if len(breweries) < per_page:
            print(f"Reached end of data at page {page}.")
            break

        # upload_to_s3(breweries, key)
        # print(breweries)
        # print('---')
        # print('---')
        # print('---')
    return number_of_breweries_found

# number_of_breweries_found = extract_to_bronze_s3(per_page=200)

# print(f"Total number of breweries loaded: {number_of_breweries_found}")

import json
import os
from pathlib import Path
import pandas as pd
from datetime import datetime

# Base input/output paths (relative to your project root)
# BRONZE_PATH = Path("./data-lake/bronze/open-brewery-api")
SILVER_PATH = Path("./data-lake/silver/breweries")

# Auto-detect the latest ingestion folder (e.g., 2025-05-13)
def get_latest_bronze_folder(bronze_base: Path) -> Path:
    folders = [f for f in bronze_base.iterdir() if f.is_dir()]
    latest_folder = max(folders, key=lambda f: f.name)
    return latest_folder

def transform_and_save_to_silver(execution_date: str = None):
    if not execution_date:
        execution_date = datetime.today().strftime("%Y-%m-%d")
    # latest_bronze_folder = get_latest_bronze_folder(BRONZE_PATH)
    # latest_bronze_folder = BRONZE_PATH 
    latest_bronze_folder = Path(f"{BRONZE_PATH}/{execution_date}")
    input_file = latest_bronze_folder / "breweries.json"

    with open(input_file, "r", encoding="utf-8") as f:
        breweries = json.load(f)

    df = pd.DataFrame(breweries)

    # Basic cleanup: drop nulls in key fields, drop redundant fields
    df = df.drop(columns=["address_2", "address_3", "street"], errors="ignore")
    df = df.dropna(subset=["id", "state"])

    # Optional: rename fields for consistency
    # df = df.rename(columns={"state": "state", "brewery_type": "type"})

    # Add an ingestion date column for tracking
    ingestion_date = latest_bronze_folder.name  # folder name is the date
    df["ingestion_date"] = ingestion_date

    # Write each state as a separate partitioned Parquet file
    for state, group_df in df.groupby("state"):
        output_dir = SILVER_PATH / f"state={state}"
        output_dir.mkdir(parents=True, exist_ok=True)

        output_file = output_dir / f"{ingestion_date}.parquet"
        group_df.to_parquet(output_file, index=False)

    print(f"✅ Transformed {len(df)} records from bronze to silver.")

# transform_and_save_to_silver()
# pd.io.parquet.get_engine('auto')