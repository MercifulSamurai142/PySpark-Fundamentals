# Databricks notebook source
# MAGIC %run "./loader_factory"

# COMMAND ----------

class AbstractLoader :
    def __init__(self, transformDF):
        self.transformDF = transformDF
    def sink(self):
        pass

class AirpodsAfterIphoneLoader(AbstractLoader):

    def sink(self):
        get_sink_source(
            sink_type = "dbfs",
            df = self.transformDF,
            path = "dbfs:/Volumes/workspace/sink_schema/sink_volume/AirpodsAfterIphone/",
            method = "overwrite"
        ).load_data_frame()