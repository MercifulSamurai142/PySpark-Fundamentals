# Databricks notebook source
# MAGIC %run "./reader_factory"

# COMMAND ----------
from reader_factory import get_data_source
class Extractor :
    """
    Abstract Class
    """
    def __init__(self):
        pass
    def extract(self):
        pass

class AirpodsAfterIphoneExtractor(Extractor):
    """
    Concrete Class
    Implement the steps for extracting or reading the data
    """
    
    def extract(self):
        customer_path = 'dbfs:/Volumes/workspace/customer_schema/customer_volume/Customer_Updated.csv'
        products_path = 'dbfs:/Volumes/workspace/products_schema/products_volume/Products_Updated.csv'
        transaction_path='dbfs:/Volumes/workspace/transaction_schema/transaction_volume/Transaction_Updated.csv'
        
        transactionInputDF = get_data_source(data_type='csv', file_path=transaction_path)\
                                .get_data_frame()
        
        customerInputDF = get_data_source(data_type='table',file_path='workspace.default.customer_delta_table_persist')\
                                .get_data_frame()
        
        print('Transaction Table order by customer_id,transaction_id')
        transactionInputDF\
            .orderBy('customer_id','transaction_id')\
            .display()

        print('Customer Table')
        customerInputDF\
            .display()
        
        inputDFs = {
            "transactionInputDF":transactionInputDF,
            "customerInputDF":customerInputDF
        }

        return inputDFs