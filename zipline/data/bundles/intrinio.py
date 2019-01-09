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

from requests.auth import HTTPBasicAuth

from six.moves.urllib.parse import urlencode
from six import iteritems
from trading_calendars import register_calendar_alias

from zipline.utils.deprecate import deprecated
from . import core as bundles
import numpy as np
import requests

import time

log = Logger(__name__)

API_CD_TIME = 1
ONE_MEGABYTE = 1024 * 1024
PAGE_SIZE = 10000

INTRINIO_DATA_URL = 'https://api.intrinio.com/prices.csv?identifier={stock}'

SP500_SYMBOLS = [ "MSFT" ,"AAPL" ,"AMZN" ,"BRK.B" ,"JNJ" ,"JPM" ,"FB"
                  ,"GOOG" ,"GOOGL" ,"XOM" ,"PFE" ,"VZ" ,"UNH" ,"PG" ,"V" ,"BAC" ,"T"
                  ,"CVX" ,"INTC" ,"WFC" ,"MRK" ,"HD" ,"CSCO" ,"KO" ,"MA" ,"BA" ,"DIS"
                  ,"CMCSA" ,"PEP" ,"MCD" ,"WMT" ,"ABBV" ,"C" ,"DWDP" ,"AMGN" ,"NFLX"
                  ,"ABT" ,"MDT" ,"ORCL" ,"MMM" ,"ADBE" ,"LLY" ,"PM" ,"IBM" ,"CRM" ,"UNP"
                  ,"PYPL" ,"HON" ,"AVGO" ,"NKE" ,"MO" ,"COST" ,"ACN" ,"TMO" ,"TXN"
                  ,"CVS" ,"GILD" ,"UTX" ,"LIN" ,"NEE" ,"NVDA" ,"BKNG" ,"SBUX" ,"BMY"
                  ,"LOW" ,"COP" ,"CAT" ,"GE" ,"USB" ,"AMT" ,"CI" ,"QCOM" ,"UPS" ,"AXP"
                  ,"CME" ,"ANTM" ,"LMT" ,"BIIB" ,"DUK" ,"DHR" ,"GS" ,"MDLZ" ,"CB" ,"BDX"
                  ,"CELG" ,"TJX" ,"ADP" ,"PNC" ,"WBA" ,"D" ,"MS" ,"EOG" ,"SLB" ,"ISRG"
                  ,"CHTR" ,"CL" ,"SPG" ,"FOXA" ,"CSX" ,"INTU" ,"SCHW" ,"SYK" ,"OXY"
                  ,"BLK" ,"DE" ,"SO" ,"BSX" ,"CCI" ,"AGN" ,"BK" ,"GD" ,"RTN" ,"EXC"
                  ,"ICE" ,"GM" ,"VRTX" ,"ILMN" ,"NOC" ,"SPGI" ,"MPC" ,"MET" ,"NSC"
                  ,"MMC" ,"ZTS" ,"KMB" ,"FDX" ,"ITW" ,"HUM" ,"PSX" ,"EMR" ,"COF" ,"ECL"
                  ,"AEP" ,"PLD" ,"CTSH" ,"MU" ,"WM" ,"ATVI" ,"TGT" ,"AON" ,"AIG" ,"BBT"
                  ,"APD" ,"AFL" ,"PRU" ,"PGR" ,"FIS" ,"HCA" ,"BAX" ,"VLO" ,"HPQ" ,"SHW"
                  ,"ROST" ,"AMAT" ,"RHT" ,"TRV" ,"F" ,"EW" ,"ADI" ,"KMI" ,"PSA" ,"SYY"
                  ,"SRE" ,"MAR" ,"ETN" ,"REGN" ,"DAL" ,"DG" ,"FISV" ,"YUM" ,"EL" ,"EQIX"
                  ,"ORLY" ,"JCI" ,"ALL" ,"WMB" ,"STZ" ,"KHC" ,"ROP" ,"ADSK" ,"LYB"
                  ,"WELL" ,"EBAY" ,"PEG" ,"TEL" ,"XEL" ,"EA" ,"STI" ,"HAL" ,"PPG" ,"EQR"
                  ,"LUV" ,"AVB" ,"STT" ,"ED" ,"GIS" ,"FOX" ,"ADM" ,"PXD" ,"APC" ,"MCO"
                  ,"GLW" ,"VFC" ,"CNC" ,"MCK" ,"APH" ,"ALXN" ,"OKE" ,"IR" ,"KR" ,"TROW"
                  ,"AZO" ,"DLTR" ,"CXO" ,"WEC" ,"DLR" ,"XLNX" ,"LRCX" ,"MTB" ,"VTR"
                  ,"PAYX" ,"ZBH" ,"PPL" ,"TWTR" ,"A" ,"HLT" ,"DFS" ,"ES" ,"PCAR" ,"DTE"
                  ,"CMI" ,"CLX" ,"PH" ,"CCL" ,"WLTW" ,"FTV" ,"MNST" ,"HPE" ,"SBAC" ,"O"
                  ,"EIX" ,"UAL" ,"NTRS" ,"NEM" ,"MSI" ,"SWK" ,"ROK" ,"FE" ,"IQV" ,"VRSK"
                  ,"BXP" ,"WY" ,"INFO" ,"CERN" ,"MKC" ,"IP" ,"SYF" ,"NUE" ,"TSN" ,"FITB"
                  ,"AWK" ,"OMC" ,"FLT" ,"APTV" ,"RCL" ,"KEY" ,"TDG" ,"GPN" ,"CHD" ,"ESS"
                  ,"CBS" ,"RSG" ,"MCHP" ,"AEE" ,"IDXX" ,"DXC" ,"HRS" ,"VRSN" ,"BLL"
                  ,"HIG" ,"ETR" ,"AME" ,"RMD" ,"AMD" ,"EVRG" ,"NTAP" ,"AMP" ,"HSY"
                  ,"FANG" ,"CTL" ,"FCX" ,"CFG" ,"FRC" ,"K" ,"FAST" ,"RF" ,"CTAS" ,"MYL"
                  ,"CNP" ,"ULTA" ,"CMS" ,"GPC" ,"WAT" ,"ABMD" ,"ALGN" ,"MXIM" ,"HCP"
                  ,"KLAC" ,"IFF" ,"CAH" ,"LLL" ,"TSS" ,"MTD" ,"CTXS" ,"EXPE" ,"HBAN"
                  ,"AJG" ,"VMC" ,"MGM" ,"LH" ,"MSCI" ,"MRO" ,"L" ,"BBY" ,"AAL" ,"PCG"
                  ,"DRI" ,"GWW" ,"HST" ,"COO" ,"DHI" ,"AAP" ,"CBRE" ,"SNPS" ,"SYMC"
                  ,"INCY" ,"ARE" ,"ABC" ,"CDNS" ,"LEN" ,"WCG" ,"ANSS" ,"PFG" ,"XYL"
                  ,"CE" ,"HRL" ,"EXPD" ,"TTWO" ,"HSIC" ,"CMA" ,"ETFC" ,"CINF" ,"EXR"
                  ,"TXT" ,"IT" ,"KSS" ,"CHRW" ,"DVN" ,"NRG" ,"LNC" ,"KMX" ,"EFX" ,"BR"
                  ,"DGX" ,"KEYS" ,"SWKS" ,"TAP" ,"CMG" ,"LW" ,"SJM" ,"UDR" ,"HES"
                  ,"CBOE" ,"MLM" ,"BHGE" ,"MAA" ,"CAG" ,"HOLX" ,"WDC" ,"VNO" ,"APA"
                  ,"ANET" ,"COG" ,"DOV" ,"VAR" ,"WYNN" ,"MOS" ,"EMN" ,"SIVB" ,"UHS"
                  ,"TSCO" ,"NOV" ,"WRK" ,"FMC" ,"FTNT" ,"REG" ,"LNT" ,"STX" ,"CPRT"
                  ,"KSU" ,"AKAM" ,"TPR" ,"CF" ,"RJF" ,"JKHY" ,"FFIV" ,"IRM" ,"AES"
                  ,"PNW" ,"VIAB" ,"NBL" ,"NI" ,"HAS" ,"M" ,"NDAQ" ,"JNPR" ,"BEN" ,"NCLH"
                  ,"MAS" ,"DRE" ,"TIF" ,"DISCK" ,"FTI" ,"RE" ,"FRT" ,"URI" ,"XRAY"
                  ,"NLSN" ,"SNA" ,"HII" ,"ZION" ,"PKI" ,"HFC" ,"NWL" ,"ARNC" ,"ALB"
                  ,"JBHT" ,"IPG" ,"PKG" ,"TMK" ,"BF.B" ,"ALLE" ,"AVY" ,"WU" ,"GRMN"
                  ,"MHK" ,"LKQ" ,"ADS" ,"ALK" ,"BWA" ,"PVH" ,"QRVO" ,"WHR" ,"JEC" ,"PHM"
                  ,"IVZ" ,"SLG" ,"AIV" ,"UNM" ,"RHI" ,"DVA" ,"FL" ,"CPB" ,"KIM" ,"AOS"
                  ,"LB" ,"DISH" ,"PNR" ,"XEC" ,"FLIR" ,"RL" ,"GPS" ,"CPRI" ,"SEE" ,"HOG"
                  ,"NKTR" ,"PBCT" ,"JWN" ,"FBHS" ,"ROL" ,"TRIP" ,"HP" ,"AMG" ,"HRB"
                  ,"JEF" ,"PRGO" ,"GT" ,"FLS" ,"AIZ" ,"MAC" ,"LEG" ,"HBI" ,"FLR" ,"XRX"
                  ,"PWR" ,"NWSA" ,"DISCA" ,"IPGP" ,"BHF" ,"UAA" ,"MAT" ,"COTY" ,"NFX"
                  ,"UA" ,"NWS"]

CUSTOM_SYMBOLS = [
    'YRD',
    'HUYA',
    ]

all_symbols = SP500_SYMBOLS + CUSTOM_SYMBOLS

def format_data_url(symbol, api_key):
    """ Build the query URL for Intrinio prices.
    """

    return INTRINIO_DATA_URL.format(stock=symbol)


def download_intrinio_price(data_url, auth, show_progress=False):
    """
    Download data from intrino stock price API, returning a pd.DataFrame
    containing all the pages of the loaded data. 
    """

    # Donwload price data
    if show_progress:
        log.info('Downloading price data @{url}'.format(url=data_url))

    data = download_without_progress(data_url, auth)
    table = pd.read_csv(data, skiprows=[0]) # Skip the first row
    table.insert(0, 'symbol', data_url.split('=')[-1])  # e.g. "https://api.intrinio.com/prices.csv?identifier=AAPL"
    table.columns = map(str.lower, table.columns)
    return table
        

def download_without_progress(url, auth):
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

    resp = requests.get(url, auth=auth)
    resp.raise_for_status()
    return BytesIO(resp.content)
    

def load_data_table(tables):
    """ Concatenate all data tables to a single one
    """
    data_table = pd.concat(tables)
    return data_table
            
def fetch_raw_data(api_key,
                   auth,
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
                data_url = format_data_url(symbol, api_key)

                table = download_intrinio_price(
                    data_url,
                    auth,
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
            'ex_dividend' : 'amount',
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

    user = environ.get('INTRINIO_USER')
    pwd = environ.get('INTRINIO_PWD')
    raw_data = fetch_raw_data(
        api_key,
        HTTPBasicAuth(user, pwd),
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
                'ex_dividend',
            ]].loc[raw_data.ex_dividend != 0],
            show_progress=show_progress
        )
    )
