import csv

import numpy as np
import pandas as pd
import requests, time
from urllib.parse import urlencode
from collections import defaultdict
from productParser import getItemOffers, parseItemInformation, getUngateStatus
from scoreCalulator import computeScores
from dotenv import load_dotenv
import os

load_dotenv()

CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
REFRESH_TOKEN = os.getenv('REFRESH_TOKEN')
SELLER_ID = os.getenv('SELLER_ID')

SP_API_URL = 'https://sandbox.sellingpartnerapi-na.amazon.com'
SP_API_URL_PROD = 'https://sellingpartnerapi-na.amazon.com'
MARKETPLACE_ID = 'ATVPDKIKX0DER'

UPCS = []
ITEM_LIST = []
UPCS_ASIN_DICT = defaultdict(list)
UPCS_COGS = defaultdict(float)


def getAmazonUrl(asin):
    return f'https://amazon.com/dp/{asin}'


def get_lwa_access_token():
    url = "https://api.amazon.com/auth/o2/token"
    form = {
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }
    r = requests.post(url, data=form, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]


def findAmazonProduct(row):
    item_name = row['Item Name']
    item_brand = row['Brand']

    query = item_name + " " + item_brand

    params = {
        'k': query
    }

    req_url = "https://www.amazon.com/s?" + urlencode(params)

    print(req_url)


def amazonProductCatalogSearch(upc, access_token):
    page_token = ''
    while True:
        headers = {
            "x-amz-access-token": access_token,
            "user-agent": "YourAppName/1.0 (Language=Python)",
            "accept": "application/json"
        }

        params = {
            'identifiers': upc,
            'identifiersType': 'UPC',
            'marketplaceIds': MARKETPLACE_ID,
            'includedData': 'identifiers,attributes',
            'pageSize': 20
        }

        if page_token != '':
            params['pageToken'] = page_token

        url = f'{SP_API_URL_PROD}/catalog/2022-04-01/items'
        res = requests.get(url, params=params, headers=headers)

        print(res.status_code, res.json())

        if res.status_code == 200:
            data = res.json()
            items = data['items']
            for item in items:
                ITEM_LIST.append(item)

            page_info = data.get('pagination')
            if page_info:
                page_token = page_info.get('nextToken')
                if page_token:
                    page_token = page_info['nextToken']
                else:
                    break
            else:
                break


def parseDataFile(file_name):
    with open(f'./data/{file_name}.csv') as f:
        reader = csv.DictReader(f)
        for row in reader:
            upc_ = row['UPC']
            if len(upc_) == 13 and upc_.startswith('0'):
                UPCS.append(upc_[1:13])


def parseItemList():
    for item in ITEM_LIST:
        asin = item.get("asin")
        identifier_list = item.get("identifiers")
        for idl in identifier_list:
            identifier_list_2 = idl.get("identifiers")
            for id2 in identifier_list_2:
                if id2.get("identifierType") == "UPC":
                    upc = id2.get("identifier")
                    if upc in UPCS:
                        UPCS_ASIN_DICT[upc].append(asin)


parseDataFile('ClearanceReport_Mesa,AZ_6862_20251104_160621')

upc_range = int(len(UPCS) / 20) if len(UPCS) % 20 == 0 else int(len(UPCS) / 20) + 1

access_token = get_lwa_access_token()
for i in range(upc_range):
    current_upcs = UPCS[i * 20:(i + 1) * 20]

    amazonProductCatalogSearch(",".join(current_upcs), access_token)
    time.sleep(1)

parseItemList()

df = pd.read_csv('./data/ClearanceReport_Mesa,AZ_6862_20251104_160621.csv', dtype={"UPC": "string"})

df = df.dropna(subset='UPC')

df["UPC"] = (
    df["UPC"].astype("string")  # keep as text
    .str.replace(r"\D", "", regex=True)
    .map(lambda x: x[1:] if len(x) == 13 and x.startswith("0") else x)
)

df['ASIN'] = df['UPC'].map(UPCS_ASIN_DICT)

df['amazon_links'] = df['ASIN'].apply(
    lambda xs: [f'https://www.amazon.com/dp/{x}' for x in xs] if isinstance(xs, (list, tuple)) else np.nan
)
mask_empty = df["ASIN"].apply(lambda x: isinstance(x, list) and len(x) == 0)
df = df[~mask_empty].copy()

df = df.explode(['ASIN', 'amazon_links']).reset_index(drop=True)

asin_data_dict = {}

for upc in UPCS_ASIN_DICT:
    asin_list = UPCS_ASIN_DICT.get(upc)
    if len(asin_list) > 0:
        for asin in asin_list:
            store_price = df.loc[df['UPC'].eq(upc), 'Clearance Price'].iat[0]
            data = getItemOffers(asin, access_token, MARKETPLACE_ID)
            retries = 0
            while data.get('errors') is not None:
                time.sleep(2)
                data = getItemOffers(asin, access_token, MARKETPLACE_ID)
                retries += 1
                if retries > 5:
                    break

            if retries > 5:
                continue

            parsed_data = parseItemInformation(data, store_price.item(), asin, MARKETPLACE_ID, access_token)
            if parsed_data.get("status") == "No Listings":
                continue

            ungate_status = getUngateStatus(asin, SELLER_ID, MARKETPLACE_ID, access_token)
            parsed_data['Ungate Status'] = ungate_status

            score1, score2 = computeScores(parsed_data)
            parsed_data['fba_score'] = score1
            parsed_data['fbm_score'] = score2

            time.sleep(2.1)

            asin_data_dict[asin] = parsed_data

df['amazon_product_information'] = df['ASIN'].map(asin_data_dict)
df = df.dropna(subset='amazon_product_information')

# unravel dict
info_cols = df['amazon_product_information'].apply(pd.Series)
df = pd.concat([df, info_cols], axis=1)

kept_cols = [
    'Item Name',
    'Category',
    'Clearance Price',
    'Retail Price',
    'Stock',
    'ASIN',
    'amazon_links',
    'buy_box_price',
    'cogs',
    'lowest_price',
    'number_of_offers',
    'competitive_price',
    'amazon_on_listing',
    'fba_sellers',
    'fbm_sellers',
    'fba_fees',
    'fbm_fees',
    'Ungate Status',
    'fba_score',
    'fbm_score'
]

df = df[kept_cols]
df = df.sort_values(by='Ungate Status', ascending=True)

df.to_csv('clearance_report_with_asin2.csv', index=False)



