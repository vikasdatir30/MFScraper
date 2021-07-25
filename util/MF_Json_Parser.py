###############################################################
#   Date        :   10-07-2021
#   Desc        :   JSON Parser class to parse config json
#   Use         :   To get all details from json, it accepts
#               :   json file.
#   Author      :   Vikas Datir
#   Modified    :
###############################################################


import json
import logging

#spark module
from pyspark.sql import SparkSession

#util module
import MF_Logger as logger


#creating new logger
log_writer = logger.config_logger(__name__, __name__,logging.INFO)


class  MF_Json_Parser:
    def __init__(self, json_path=""):
        self.pipe_json_file = json_path
        try:
            # reading json file
            with open(self.pipe_json_file) as fptr:
                self.pipe_config = json.load(fptr)
            # filling variable with json details
            self.source_config = self.pipe_config.get("source")
            self.stage_config = self.pipe_config.get("stage")
            self.transform_config = self.pipe_config.get("transform")
            self.target_config = self.pipe_config.get("target")
            self.spark_config =self.pipe_config.get("spark")
            log_writer.info("Json object filled with details ..!")
        except Exception as e:
            log_writer.error("#" * 5 + str(e))

    def get_spark_session(self):
        # creating spark session
        self.spark_obj = SparkSession.builder.master(self.spark_config.get("master"))\
                .appName(self.spark_config.get("app_name")).getOrCreate()

        log_writer.info(f"Spark session created with details {0}".self.spark_config)
        return self.spark_obj


if __name__ == '__main__':
    print('This should not be run as main')