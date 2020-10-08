"""
    This class is responsible for acquiring the OHLC and Google Trends Data.
    Project Boostrade started by @denizsurmeli in 29.08.2020
"""

import datetime
import psycopg2
import numpy
import pandas
import time
import json

from pytrends.request import TrendReq
from binance.client import Client


class BaseDataCollector:
    def __init__(self,credentialsJSON,pair:str):
        """
            The __init__ method needs one parameter, which is a JSON object to read credentials for Binance API and database access.

                :param credentialsJSON: Path to JSON object.
                :param pair: Name of the cryptocurrency pair.

            @IMPORTANT NOTE:
                PARAMETERS MUST BE PASSED WITH A JSON OBJECT IN THIS FORMAT:
                    {
                        "userParams":{
                                        "binanceApiPublic":"xxxxxxxxxxxxxx",
                                        "binanceApiSecret":"xxxxxxxxxxxxxx",
                                        "postgresUserName":"xxxxxxxxxxxxxx",
                                        "postgresDatabaseName":"xxxxxxxxxxxxxx",
                                        "postgresPassword":"xxxxxxxxxxxxxx"
                                    }
                    }


            @IMPORTANT NOTE:
                The database used by the program is LOCAL.
                @user must be carefully implement and take care of conflicts etc.
        """
        loadFile = open(credentialsJSON)
        loadData = json.load(loadFile)
        self.pair = pair.upper()
        self.binanceApiPublicKey = loadData["userData"]["binanceApiPublic"]
        self.binanceApiSecretKey = loadData["userData"]["binanceApiSecret"]
        self.postgresUserName = loadData["userData"]["postgresUserName"]
        self.postgresDatabaseName = loadData["userData"]["postgresDatabaseName"]
        self.postgresPassword = loadData["userData"]["postgresPassword"]
        self.isTableCreated = False

    def initializeBinanceClient(self):
        """
            Broker initializer.
            @NOTE:
                The main reason why initialization is formed as function due to error handling.
                Since we use an external API to broker's systems, we might need to restart services
                or encounter undefined/strange behaviour.
            @TODO:Need proper handles for processes to run synchronized.
            :return: Boolean whether the process is successful or not.
        """
        try:
            self.binanceClient = Client(self.binanceApiPublicKey,
                                        self.binanceApiSecretKey)
            return True
        except (Exception) as err:
            print(err)
            print("An error occurred while connecting to the Binance API.")


    def connectToDatabase(self):
        """
            Database connector.
            @NOTE:
                Local host must be running 24/7 for proper data collection.
                It is recommended to run scripts in an AWS instance or within a similar service.
            @NOTE:
                Since this function is an initializer, it will not return anything. However it can stop
                or restart the services.
            @TODO:Need proper handles for processes to run synchronized.

            :return: Boolean whether the process is successful or not.
        """
        try:
            databaseConnectionQuery = "dbname="+self.postgresDatabaseName+" user="+self.postgresUserName+" password="+self.postgresPassword
            self.databaseConnection = psycopg2.connect(databaseConnectionQuery)
            self.databaseCursor = self.databaseConnection.cursor()
            print("Connected to database.")
            return True
        except (Exception) as err:
            print(err)
            print("An error ocurred while connecting to the localdatabase.")

    def createTable(self,areSystemsRunning):
        """
        :param areSystemsRunning: Is a boolean that indicates that initializer functions are running correctly.
        :return:
        """

        if(areSystemsRunning):
            try:
                query = "CREATE TABLE "+self.pair.lower()+" (date TIMESTAMP,open NUMERIC,high NUMERIC,low NUMERIC,close NUMERIC,volume NUMERIC,googletrends NUMERIC);"
                self.databaseCursor.execute(query)
                print("Table -> " + self.pair  + " created.")
                self.isTableCreated = True
            except (Exception) as err:
                print(err)
                print("An error occured while creating the table.")


    def writeDataToColumn(self,candlestick,date,momentTrendValue=-1):
        """
        :param candlestick:
        :param date:
        :return:
        """
        if(self.isTableCreated):
            tempCandle = "'" +str(date)+ "'" + ","
            for i in range(1,6):
                tempCandle += candlestick[i]+","
            tempCandle += str(momentTrendValue)
            query="INSERT INTO "+self.pair.lower()+" VALUES("+tempCandle+");"
            self.databaseCursor.execute(query)


    def runInitializers(self):
        try:
            self.initializeBinanceClient()
            self.connectToDatabase()
            return True
        except (Exception) as err:
            print(err)
            print("An error occured while trying to initialize connections.")
            return False

    # def fetchData(self,convoPairName,interval=Client.KLINE_INTERVAL_1HOUR,timeframe="now 7-d",limit = 144 , goToSleep = 60*60*60):
    #
    #     trendRequest = TrendReq(hl="en-US")
    #     createKeyword = "buy " + convoPairName
    #     trendRequest.build_payload([createKeyword],cat=0,timeframe=timeframe)
    #     trendValues = trendRequest.interest_over_time()
    #     klines = self.binanceClient.get_historical_klines(self.pair,
    #                                                           interval,
    #                                                           "7 day ago UTC",
    #                                                           limit = 6)
    #     for i in range(len(klines)):
    #         date = datetime.datetime.fromtimestamp(klines[i][0]/1000.0)
    #         try:
    #             momentTrendValue = trendValues.loc[str(date),createKeyword]
    #         except:
    #             continue
    #         self.writeDataToColumn(klines[i],date,momentTrendValue=momentTrendValue)
    #         self.databaseConnection.commit()
    #     time.sleep(goToSleep-2)
    #     self.fetchData(convoPairName,interval,timeframe)



    def startProcess(self,convoPairName,interval=Client.KLINE_INTERVAL_1HOUR,timeframe="now 7-d",limit = 144 , goToSleep = 0):
        """
        @IMPORTANT NOTE:
            Since the Google Trends Data available in a different form of time factor , the process
            will decide the proper interval for matching data. Recommended usage is Hourly candles/Weekly Trend
            data.
        :param timeframe: Timeframe for trends data frequency.
        :param convoPairName: Pair's name for running trend search. For example : "BTCUSDT" -> pair "bitcoin" -> convoPairName
        :param interval: Default 1H candles. Interval for data fetching. Changing the interval at the moment is not recommended.
        :return:
        """


        if(not self.isTableCreated):
            self.createTable(self.runInitializers())
        trendRequest = TrendReq(hl="en-US")
        createKeyword = "buy " + convoPairName
        trendRequest.build_payload([createKeyword],cat=0,timeframe=timeframe)
        trendValues = trendRequest.interest_over_time()
        klines = self.binanceClient.get_historical_klines(self.pair,
                                                              interval,
                                                              "7 day ago UTC",
                                                              limit = limit)

        for i in range(len(klines)):
            date = datetime.datetime.fromtimestamp(klines[i][0]/1000.0)
            try:
                momentTrendValue = trendValues.loc[str(date),createKeyword]
            except:
                continue
            self.writeDataToColumn(klines[i],date,momentTrendValue=momentTrendValue)
            self.databaseConnection.commit()

        time.sleep(goToSleep)
        goToSleep = 60*60*60
        self.startProcess(convoPairName,interval,timeframe,6,goToSleep)



