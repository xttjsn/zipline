"""
Module for building dataset from Intrinio's data
"""
from io import BytesIO
import tarfile
from zipfile import ZipFile

from click import progressbar
from logbook import Logger
import pandas as pd
import requests
from six.moves.urllib.parse import urlencode
from six import iteritems
from trading_calendars import register_calendar_alias

from zipline.utils.deprecate import deprecated
from . import core as bundles
import numpy as np
import requests
import json
import time

log = Logger(__name__)

API_CD_TIME = 1
ONE_MEGABYTE = 1024 * 1024
PAGE_SIZE = 10000
INTRINIO_DATA_URL = 'https://api-v2.intrinio.com/securities/{stock}/prices?'
INTRINIO_ADJ_URL = 'https://api-v2.intrinio.com/securities/{stock}/prices/adjustments?'

SP500_SYMBOLS_50 = [
    'MSFT',
    'AAPL',
    'AMZN',
    'BRK.B',
    'JNJ',
    'JPM',
    'FB',
    'GOOG', # Alphabet Inc. Class C
    'GOOGL',# Alphabet Inc. Class A
    'XOM',  # Exxon Mobile Corporation
    'PFE',  # Pfizer Inc.
    'UNH',  # UnitedHealth Group Inc.
    'V',    # Visa Inc. Class A
    'VZ',   # Verizon Communications Inc.
    'BAC',  # Bank of America Corp
    'PG',   # Procter & Gamble Company
    'T',    # AT&T Inc.
    'INTC', # Intel Corporation
    'CVX',  # Chevron Corporation
    'WFC',  # Wells Fargo & Company
    'MRK',  # Merck & Co. Inc.
    'HD',   # Home Depot Inc.
    'CSCO', # Cisco Systems Inc.
    'KO',   # Coca-Cola Company
    'MA',   # Mastercard Incorporated Class A
    'BA',   # Boeing Company
    'DIS',  # Walt Disney Company
    'CMCSA',# Comcast Corporation Class A
    'PEP',  # PepsiCo Inc.
    'MCD',  # McDonald's Corporation
    'ABBV', # AbbVie Inc.
    'WMT',  # Walmart Inc.
    'C',    # Citigroup Inc.
    'DWDP', # DowDuPont Inc.
    'AMGN', # Amgen Inc.
    'ABT',  # Abbott Laborotories
    'MDT',  # Medtronic plc
    'NFLX', # Netflix Inc.
    'ORCL', # Oracle Corporation
    'MMM',  # 3M Company
    'ADBE', # Adobe
    'LLY',  # Eli Lilly and Company
    'AVGO', # Broadcom Inc.
    'PM',   # Philip Morris International Inc.
    'IBM',  # International Business Machines Corporation
    'CRM',  # Salesforce.com inc
    'UNP',  # Union Pacific Corporation
    'PYPL', # PayPal Holdings Inc
    'HON',  # Honeywell International Inc.
    'NKE',  # NIKE Inc. Class B
    ]

CUSTOM_SYMBOLS = [
    'YRD'
    ]

all_symbols = SP500_SYMBOLS_50 + CUSTOM_SYMBOLS

def format_data_url(symbol, api_key):
    """ Build the query URL for Intrinio prices.
    """
    query_params = [('api_key', api_key), ('page_size', PAGE_SIZE)]
    return (INTRINIO_DATA_URL + urlencode(query_params)).format(stock=symbol),\
        (INTRINIO_ADJ_URL + urlencode(query_params)).format(stock=symbol)

def download_intrinio_price(data_url, adj_url, show_progress=False):
    """
    Download data from intrino stock price API, returning a pd.DataFrame
    containing all the pages of the loaded data. 
    """
    tables = []
    params = {}
    json_data = None
    prev_page = 'abcdefg'  # Random value

    def paramdeco(params):
        if params:
            return '&' + params
        return ''
    
    # Donwload price data
    while True:
        if show_progress:
            log.info('Downloading a page of price data @{url}'.format(url=data_url + paramdeco(urlencode(params))))

        data = download_without_progress(data_url + paramdeco(urlencode(params)))
        json_data = json.loads(data.read())
        tables.append(pd.DataFrame.from_dict(json_data['stock_prices']))
        log.info("Sleep for {t} seconds to CD the api call".format(t=API_CD_TIME))
        time.sleep(API_CD_TIME)
        if json_data['next_page'] != None:
            params['next_page'] = json_data['next_page']
        else:
            break
        
    table = pd.concat(tables)
    table.insert(0, 'symbol', json_data['security']['ticker'])
    table.insert(1, 'asset_name', json_data['security']['name'])

    # Download adjustment data
    if show_progress:
        log.info('Downloading adjustment data @{url}'.format(url=adj_url))

    data = download_without_progress(adj_url)
    json_data = json.loads(data.read())
    adj_table = pd.DataFrame.from_dict(json_data['stock_price_adjustments'])
    if not adj_table.empty:
        table = pd.merge(table, adj_table, how='outer', on='date')
        table.fillna(value={'dividend' : 0,
                            'dividend_currency' : 'USD',
                            'factor' : 1,
                            'split_ratio' : 1.0}, inplace=True)
    else:
        table['dividend'] = 0
        table['dividend_currency'] = 'USD'
        table['factor'] = 1
        table['split_ratio'] = 1.0
        
    return table
        

def download_without_progress(url):
    """
    Download data from a URL, returning a BytesIO containing the loaded data.

    Parameters
    ----------
    url : str
        A URL that can be understood by ``requests.get``.

    Returns
    -------
    data : BytesIO
        A BytesIO containing the downloaded data.
    """
    resp = requests.get(url)
    resp.raise_for_status()
    return BytesIO(resp.content)
    

def load_data_table(tables):
    """ Concatenate all data tables to a single one
    """
    data_table = pd.concat(tables)
    return data_table
            
def fetch_raw_data(api_key,
                   show_progress,
                   retries):
    """ Fetch raw price data from Intrinio
    """
    tables = []
    for _ in range(retries):
        try:
            if show_progress:
                log.info('Downloading Equity data.')
            for symbol in all_symbols:
                data_url, adj_url = format_data_url(symbol, api_key)
                table = download_intrinio_price(
                    data_url,
                    adj_url,
                    show_progress
                )

                tables.append(table)

                log.info("Sleep for {t} seconds to CD the api call".format(t=API_CD_TIME))
                time.sleep(API_CD_TIME)

            return load_data_table(tables)

        except Exception:
            log.exception("Exception raised reading Intrinio data. Retrying.")

    else:
        raise ValueError(
            "Failed to download Intrinio data after %d attempts." % (retries)
        )

def gen_asset_metadata(data, show_progress):
    if show_progress:
        log.info('Generating asset metadata.')

    data = data.groupby(
        by='symbol'
    ).agg(
        {'date' : [np.min, np.max]}
    )
    data.reset_index(inplace=True)
    data['start_date'] = data.date.amin
    data['end_date'] = data.date.amax
    del data['date']
    data.columns = data.columns.get_level_values(0)

    data['exchange'] = 'INTRINIO'
    data['auto_close_date'] = data['end_date'].values.astype(np.datetime64) + pd.Timedelta(days=1)
    return data

def parse_pricing_and_vol(data,
                          sessions,
                          symbol_map):
    for asset_id, symbol in iteritems(symbol_map):
        asset_data = data.xs(
            symbol,
            level=1
        ).reindex(
            sessions.tz_localize(None)
        ).fillna(0.0)
        yield asset_id, asset_data

def parse_splits(data, show_progress):
    if show_progress:
        log.info('Parsing split data.')

    data['split_ratio'] = 1.0 / data.split_ratio
    data.rename(
        columns={
            'split_ratio' : 'ratio',
            'date' : 'effective_date',
        },
        inplace=True,
        copy=False
    )
    return data

def parse_dividends(data, show_progress):
    if show_progress:
        log.info('Parsing dividends.')

    data['record_date'] = data['declared_date'] = data['pay_date'] = pd.NaT

    data.rename(
        columns={
            'dividend' : 'amount',
            'date' : 'ex_date',
        },
        inplace=True,
        copy=False,
    )
    return data
        
@bundles.register('intrinio')
def intrinio_bundle(environ,
                    asset_db_writer,
                    minute_bar_writer,
                    daily_bar_writer,
                    adjustment_writer,
                    calendar,
                    start_session,
                    end_session,
                    cache,
                    show_progress,
                    output_dir):
    """
    intrinio_bundle builds a daily dataset using Intrinio's equities api.
    """
    api_key = environ.get('INTRINIO_API_KEY')
    if api_key is None:
        raise ValueError(
            'Please set your INTRINIO_API_KEY environment variable and retry')

    raw_data = fetch_raw_data(
        api_key,
        show_progress,
        environ.get('INTRINIO_DOWNLOAD_ATTEMPTS', 5)
    )

    asset_metadata = gen_asset_metadata(
        raw_data[['symbol', 'date']],
        show_progress
    )

    asset_db_writer.write(asset_metadata)

    symbol_map = asset_metadata.symbol
    sessions = calendar.sessions_in_range(start_session, end_session)

    raw_data.set_index(['date', 'symbol'], inplace=True)
    daily_bar_writer.write(
        parse_pricing_and_vol(
            raw_data,
            sessions,
            symbol_map
        ),
        show_progress=show_progress
    )

    raw_data.reset_index(inplace=True)
    raw_data['symbol'] = raw_data['symbol'].astype('category')
    raw_data['sid'] = raw_data.symbol.cat.codes

    print('raw_data : {}'.format(raw_data.head()))
    
    adjustment_writer.write(
        splits=parse_splits(
            raw_data[[
                'sid',
                'date',
                'split_ratio',
            ]].loc[raw_data.split_ratio != 1],
            show_progress=show_progress
        ),
        dividends=parse_dividends(
            raw_data[[
                'sid',
                'date',
                'dividend',
            ]].loc[raw_data.dividend != 0],
            show_progress=show_progress
        )
    )
