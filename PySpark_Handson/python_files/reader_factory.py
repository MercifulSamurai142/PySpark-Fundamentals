# Databricks notebook source
class DataSource :
    """
    Abstract Class
    """
    def __init__(self,path) :
        self.path = path

    def get_data_frame(self) :
        """
        Abstract Method, Function will be implemented in the child class
        """
        raise ValueError("Abstract method not implemented")

class CSVDataSource(DataSource) :
    def get_data_frame(self) :
        return (
            spark.read.format('csv')\
                .option('header',True)\
                .load(self.path)
        )

class ParaquetDataSource(DataSource) :
    def get_data_frame(self) :
        return (
            spark.read.format('parquet')\
                .load(self.path)
        )

class DeltaDataSource(DataSource) :
    def get_data_frame(self) :
        return (
            spark.read.format('delta')\
                .load(self.path)
        )

class TableDataSource(DataSource) :
    def get_data_frame(self) :
        return (
            spark.read.table(self.path)
        )

def get_data_source(data_type, file_path) :
    if data_type == 'csv' :
        return CSVDataSource(file_path)
    elif data_type == 'parquet' :
        return ParaquetDataSource(file_path)
    elif data_type == 'delta' :
        return DeltaDataSource(file_path)
    elif data_type == 'table' :
        return TableDataSource(file_path)
    else :
        raise ValueError(f"Not implemented data type: {data_type}")