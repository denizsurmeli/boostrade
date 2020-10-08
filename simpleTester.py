import datetime
import psycopg2
import numpy
import pandas
import json

from pytrends.request import TrendReq
from binance.client import Client

"""
We will test that if we can match the timezones and data correctly:
Google Trends API will be our north star for the trading strategy,
Binance API will be used as our Cryptocurrency exchange, hence the data will be retrieved from them.
EVERY TRANSACTION WILL BE PAIRED AS XXXX/USDT OR XXXX/USD.
PORTFOLIO OPTIMIZATION WILL NOT BE A PART OF OUR PROTOTYPE.
Backtesting AND testing will be implemented later on.

IT MUST be known that the first developer @denizsurmeli is a very inexperienced developer and 
projects developed by him must be used/inspired carefully.
"""

# These database connections are using for test purposes only.
# The other developers must use a local host with different credentials.
# The variables must be monitored for leaks.
dbconn = psycopg2.connect("dbname=testdb user=postgres password=h2nru4ye")
dbcur = dbconn.cursor()

apiDataFile = open("credentials.json")
apiData = json.load(apiDataFile)

commodityTrends = TrendReq(hl='en-US')
client = Client(str(apiData['userData']['binanceApiPublic']),
                str(apiData['userData']['binanceApiSecret']))

# keyword_list = ["buy bitcoin"]
#
# commodityTrends.build_payload(keyword_list, cat=0, timeframe="now 7-d")
#
# # dbcur.execute("CREATE TABLE gtbtcusdt (timestamp,timestamp,numeric,numeric,numeric,numeric,numeric,numeric);")
# bitcoinTrend = commodityTrends.interest_over_time()
klines = client.get_historical_klines("BTCUSDT",
                                      Client.KLINE_INTERVAL_1HOUR,
                                      "7 day ago UTC",
                                      limit=6)
print(str(datetime.datetime.fromtimestamp(klines[-1][0]/1000.0)))

