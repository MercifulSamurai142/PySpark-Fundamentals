# Databricks notebook source
class DataSink :
    """
    Abstract Class
    """
    def __init__(self, df, path, method, params = None) :
        self.df = df
        self.path = path
        self.method = method
        self.params = params

    def load_data_frame(self) :
        """
        Abstract Method, Function will be implemented in the child class
        """
        raise ValueError("Abstract method not implemented")

class LoadToDBFS(DataSink):

    def load_data_frame(self):

        self.df.write.mode(self.method).save(self.path)

class LoadToDBFSWithPartition(DataSink):

    def load_data_frame(self):
        partitionByColumns = self.params.get("partitionByColumns")
        self.df.write.mode(self.method).partitionBy(*partitionByColumns).save(self.path)

def get_sink_source(sink_type, df, path, method, params=None):
    if(sink_type == 'dbfs'):
        return LoadToDBFS(df, path, method, params)
    elif(sink_type == 'dbfs_with_partition'):
        return LoadToDBFSWithPartition(df, path, method, params)
    else:
        raise ValueError(f"Not implemented for sink type: {sink_type}")