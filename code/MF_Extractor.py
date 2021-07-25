###############################################################
#   Date        :   25-07-2021
#   Desc        :   Data extraction class
#   Use         :   to extract data from the given source url
#               :   It accepts MF_Json_Parser object
#   Author      :   Vikas Datir
#   Modified    :
###############################################################

import logging
import pandas as pd
import requests
import functools
from bs4 import BeautifulSoup
from requests.exceptions import RequestException
from pyspark.sql import DataFrame

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from pyspark.sql.types import DateType
from datetime import datetime as dt

# util module
import MF_Logger as logger
log_writer = logger.config_logger(__name__, __name__, logging.INFO)


class MF_Extractor(object):
    def __init__(self, json_parser_obj=""):
        log_writer.info("MF_Extractor started ..!")
        # print(json_parser_obj.source_config)
        self.source_url = json_parser_obj.source_config
        self.stage_db = json_parser_obj.stage_config
        self.spark = json_parser_obj.get_spark_session()

    def data_extract(self):
        try:
            log_writer.info("Data extraction started ..!")
            # getting data from website
            self.response = requests.get(self.source_url.get('url'))
            if self.response.status_code == 200 and self.response.status_code != 400:
                log_writer.info("Data extraction completed ..!")
                return pd.read_html(self.response.text)
            else:
                log_writer.info(f"Not Found - {0}".self.source_url.get('url'))
        except RequestException as e:
            log_writer.error("#" * 5 + str(e))

    def url_extract(self):
        try:
            url_list = []
            log_writer.info("Url extraction started ..!")
            # getting html code from website
            self.raw_html_data = BeautifulSoup(requests.get(self.source_url.get('url')).content, "html.parser")
            for url in self.raw_html_data.find_all('a'):
                url_list.append((url.get('title'), url.get('href')))
            log_writer.info("Url extraction completed ..!")
            return url_list
        except RequestException as e:
            log_writer.error("#" * 5 + str(e))

    def source_extract(self):
        log_writer.info("Source extraction started ..!")
        self.url_data = self.url_extract()
        self.raw_data = self.data_extract()
        dataframe_list = []
        # reading only required dataframes from raw data
        for i in range(0, len(self.raw_data) - 16):  # range(0,len(self.raw_data)-16):
            cols = self.raw_data[i].columns
            column_names = list(map(lambda x: x.replace(" ", "_"), cols))

            raw_df = self.spark.createDataFrame(self.raw_data[i], schema=column_names)
            cat_df = raw_df.withColumn("Category", lit(column_names[0])) \
                .withColumn("Extract_Datetime", lit(dt.now().strftime("%d-%m-%Y %H:%M")))
            dataframe_list.append(cat_df)

        # merging all dataframes into one
        union_df = functools.reduce(DataFrame.union, dataframe_list)

        # renaming the first column of the datafarme
        fund_data_df = union_df.withColumnRenamed(union_df.columns[0], "Fund_Name").withColumnRenamed(
            union_df.columns[2], "aum_cr")

        # create a dataframe for holding urls data
        url_df = self.spark.createDataFrame(self.url_data, schema=['Title', 'Link'])

        # getting urls for all funds
        self.load_read_data = fund_data_df.join(url_df, url_df["Title"] == fund_data_df["Fund_Name"], "left")

        log_writer.info("Source extraction completed ..!")

    def load_to_stage(self):
        log_writer.info("Stage load started ..!")
        self.load_read_data.write.format('jdbc').options(
            url=self.stage_db.get('db_server_url'),
            driver=self.stage_db.get('driver'),
            dbtable=self.stage_db.get('dbtable'),
            user=self.stage_db.get('user'),
            password=self.stage_db.get('password')).mode('append').save()
        log_writer.info("Stage load completed ..!")


if __name__ == '__main__':
    print('This should not be run as main')
