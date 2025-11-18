from bs4 import BeautifulSoup
import requests
import time

# information needed
# Sales per month
# price
# number of sellers
# amazon on buy box
# sales rank


def getUngateStatus(asin, seller_id, marketplace_id, access_token):
    url = f"https://sellingpartnerapi-na.amazon.com/listings/2021-08-01/restrictions?asin={asin}&conditionType=new_new&sellerId={seller_id}&marketplaceIds={marketplace_id}"

    headers = {
        "x-amz-access-token": access_token,
        "user-agent": "YourAppName/1.0 (Language=Python)",
        "accept": "application/json"
    }

    response = requests.get(url, headers=headers)

    data = response.json()

    print(data)

    if data:
        restrictions = data.get('restrictions')
        if restrictions:
            return 'Restricted'
        else:
            return 'Not Restricted'

    return 'Restricted'


def getItemOffers(asin, access_token, marketplace_id):
    url = f'https://sellingpartnerapi-na.amazon.com/products/pricing/v0/items/{asin}/offers'

    headers = {
        "x-amz-access-token": access_token,
        "user-agent": "YourAppName/1.0 (Language=Python)",
        "accept": "application/json"
    }

    params = {
        'MarketplaceId': marketplace_id,
        'ItemCondition': 'New'
    }

    response = requests.get(url, headers=headers, params=params)

    return response.json()


def getFeeInformation(asin, list_price, marketplace_id, access_token):
    url = f"https://sellingpartnerapi-na.amazon.com/products/fees/v0/items/{asin}/feesEstimate"

    params_fbm = {
        "FeesEstimateRequest": {
            "PriceToEstimateFees": {
                "ListingPrice": {"CurrencyCode": "USD", "Amount": list_price},
                "Shipping": {"CurrencyCode": "USD", "Amount": 0.0},
            },
            "IsAmazonFulfilled": False,
            "MarketplaceId": marketplace_id,
            "Identifier": "0"
        }
    }

    params_fba = {
        "FeesEstimateRequest": {
            "PriceToEstimateFees": {
                "ListingPrice": {"CurrencyCode": "USD", "Amount": list_price},
                "Shipping": {"CurrencyCode": "USD", "Amount": 0.0},
            },
            "IsAmazonFulfilled": True,
            "MarketplaceId": marketplace_id,
            "Identifier": "0"
        }
    }

    headers = {
        "x-amz-access-token": access_token,
        "accept": "application/json",
        "content-type": "application/json"
    }

    response_fbm = requests.post(url, json=params_fbm, headers=headers)
    response_fba = requests.post(url, json=params_fba, headers=headers)

    if response_fbm.json().get('errors'):
        print('fbm')
        time.sleep(2)
        response_fbm = requests.post(url, json=params_fbm, headers=headers)

    if response_fba.json().get('errors'):
        print('fba')
        time.sleep(2)
        response_fba = requests.post(url, json=params_fba, headers=headers)

    return response_fba.json(), response_fbm.json()


def parseItemInformation(data, price, asin, marketplace_id, access_token):
    print(data)
    to_return = {
        'buy_box_price': 0.0,
        'cogs': price,
        'lowest_price': 0.0,
        'number_of_offers': 0,
        'competitive_price': 0.0,
        'amazon_on_listing': False,
        'fba_sellers': 0,
        'fbm_sellers': 0,
        'fba_fees': 0.0,
        'fbm_fees': 0.0,
        'sales_ranks': []
    }

    payload = data.get('payload')
    if payload['status'] != 'Success':
        return {'status': 'No Listings'}
    summary = payload.get('Summary')

    lowest_pricing = summary.get('LowestPrices')
    if lowest_pricing is not None:
        for lp in lowest_pricing:
            listing_price = lp.get('ListingPrice')
            amount = listing_price.get('Amount')
            to_return['lowest_price'] = amount

    buy_box_pricing = summary.get('BuyBoxPrices')
    if buy_box_pricing is not None:
        for bb in buy_box_pricing:
            listing_price = bb.get('ListingPrice')
            amount = listing_price.get('Amount')
            to_return['buy_box_price'] = amount

    number_offers = summary.get('NumberOfOffers')
    if number_offers is not None:
        for n in number_offers:
            if n.get('fulfillmentChannel') == "Merchant":
                to_return['fbm_sellers'] = n.get('OfferCount')
            elif n.get('fulfillmentChannel') == "Amazon":
                to_return['fba_sellers'] = n.get('OfferCount')

    sales_rankings = summary.get('SalesRankings')
    if sales_rankings is not None:
        to_return['sales_ranks'] = sales_rankings

    competitive_price = summary.get('CompetitivePriceThreshold')
    if competitive_price is not None:
        to_return['competitive_price'] = competitive_price.get('Amount')

    num_offers = summary.get('TotalOfferCount')
    if num_offers is not None:
        to_return['number_of_offers'] = num_offers

    offers = payload.get('Offers')
    if offers is not None:
        for offer in offers:
            if offer.get('SellerId') == 'ATVPDKIKX0DER':
                to_return['amazon_sellers'] = True

    fba_fees, fbm_fees = getFeeInformation(asin, to_return.get('lowest_price'), marketplace_id, access_token)
    print(fba_fees)
    print(fbm_fees)

    fee_amount_fba = 0
    fee_payload_fba = fba_fees.get('payload')
    if fee_payload_fba is not None:
        fee_estimate_result_fba = fee_payload_fba.get('FeesEstimateResult')
        if fee_estimate_result_fba.get('Status') == 'Success':
            fees_estimate_fba = fee_estimate_result_fba.get('FeesEstimate')
            total_estimate_fba = fees_estimate_fba.get('TotalFeesEstimate')
            fee_amount_fba = total_estimate_fba.get('Amount')

    to_return['fba_fees'] = fee_amount_fba

    fee_payload_fbm = fbm_fees.get('payload')
    fee_amount_fbm = 0
    fee_estimate_result_fbm = fee_payload_fbm.get('FeesEstimateResult')
    if fee_estimate_result_fbm.get('Status') == 'Success':
        fees_estimate_fbm = fee_estimate_result_fbm.get('FeesEstimate')
        total_estimate_fbm = fees_estimate_fbm.get('TotalFeesEstimate')
        fee_amount_fbm = total_estimate_fbm.get('Amount')

    to_return['fbm_fees'] = fee_amount_fbm
    to_return['status'] = 'Listings Available'

    return to_return

