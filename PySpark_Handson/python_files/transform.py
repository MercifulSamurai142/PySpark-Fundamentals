# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import lead,col,broadcast

class Transformer :
    def __init__(self):
        pass
    def transform(self):
        pass
class FirstTransformer(Transformer):
    def transform(self, inputDFs):
        """
        Customers who have bought Airpods after buying the iPhone
        """
        transactionInputDF = inputDFs.get('transactionInputDF')
        # print("Transaction Data")
        # transactionInputDF.display()
        
        #LEAD(product_name) -> Partition By customer_id & order by transaction_id
        windowSpec = Window.partitionBy('customer_id')\
                            .orderBy('transaction_date')
        transformedDF = transactionInputDF.withColumn(
            'next_product_name',lead('product_name').over(windowSpec)
        )
        print("Successive purchased product using lead function")
        transformedDF.orderBy('customer_id','transaction_date','product_name').display()

        filteredDF = transformedDF.filter(
            (col('product_name') == 'iPhone') & (col('next_product_name') == 'AirPods')
        )

        print("Customers buying Airpods after iphone")
        filteredDF.orderBy('customer_id','transaction_date','product_name').display()

        customerInputDF = inputDFs.get('customerInputDF')
        # customerInputDF.display()
        joinDF = customerInputDF.join(
            broadcast(filteredDF),
            'customer_id'
        )
        print('JOINED DF to display customer info')
        joinDF.display()
        return joinDF.select(
            'customer_id',
            'customer_name',
            'location'
        )