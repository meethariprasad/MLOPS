# Databricks notebook source
from runtime.nutterfixture import NutterFixture, tag
import json
class NutterDemoTestFixture(NutterFixture):
  #share state across methods using self
  def __init__(self):
    self.retVal2={}
    self.testDF = None
    NutterFixture.__init__(self)
  
  def before_testTripCount(self):
    self.testDF = spark.read.option("header","true").option("inferSchema","true").csv("/mnt/workshop/nyc-trip-data/test")
    
  def run_testTripCount(self):
    retVal = dbutils.notebook.run('proc_nycdata_notebook', 600, {"filePath":"/mnt/workshop/nyc-trip-data/test","tripMonthView":"tripMonthView","medallion":"0BD7C8F5BA12B88E0B67BED28BEA73D8","month":1})
    self.retVal2 = json.loads(retVal)
  
  def assertion_testTripCount(self):
    expctedCount = self.testDF.count()
    print(self.retVal2)
    print(expctedCount)
    assert(self.retVal2["total_rec"] == expctedCount)
  
  def assertion_testMonthColumn(self):
    #df_col_check = spark.sql("SELECT * FROM tripMonthView")
    df_col_check = spark.sql("select * from global_temp.tripMonthView")
    assert("pickupMonth" in df_col_check.columns)
    
  def run_testTotalPassengers(self):
    retVal = dbutils.notebook.run('nycdata_proc_notebook', 600, {"filePath":"/mnt/workshop/nyc-trip-data/test","tripMonthView":"tripMonthView","medallion":"DFD2202EE08F7A8DC9A57B02ACB81FE2","month":1})
    self.retVal2 = json.loads(retVal)
    
  def assertion_testTotalPassengers(self):
    assert(self.retVal2["totalPass"] == 2)
    
result = NutterDemoTestFixture().execute_tests()
print(result.to_string())

# COMMAND ----------

  retVal = dbutils.notebook.run('nycdata_proc_notebook', 600, {"filePath":"/mnt/workshop/nyc-trip-data/test","tripMonthView":"tripMonthView"})
  print(retVal)
