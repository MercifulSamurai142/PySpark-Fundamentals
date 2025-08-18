# Databricks notebook source
# MAGIC %run "./transform"

# COMMAND ----------

# MAGIC %run "./extractor"

# COMMAND ----------

# MAGIC %run "./loader"

# COMMAND ----------

class FirstWorkFlow :
    """
    ETL pipeline to generate the data of all customers who bought airpods after buying the iPhone
    """
    def __init__(self) :
        pass

    def runner (self) :
        
        #Step 1 : Extract all required data
        inputDFs = AirpodsAfterIphoneExtractor().extract()

        #Step 2 : Transform : Customers who have bought Airpods after buying the iPhone
        firstTransformDF = FirstTransformer().transform(inputDFs)
        # firstTransformDF.display()
        #Step 3 : Load : Save the output to a table
        AirpodsAfterIphoneLoader(firstTransformDF).sink()

# COMMAND ----------

class WorkFlowRunner :
    def __init__(self,name):
        self.name = name

    def work_runner(self):
        if self.name == 'FirstWorkFlow':
            return FirstWorkFlow().runner()

name = "FirstWorkFlow"
workflowRunner = WorkFlowRunner(name).work_runner()