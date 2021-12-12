# Databricks notebook source
#/mnt/workshop/nyc-trip-data/trip

# COMMAND ----------

# MAGIC %fs ls /mnt/workshop/nyc-trip-data/test

# COMMAND ----------

dbutils.widgets.text("filePath","/mnt/workshop/nyc-trip-data/trip")
dbutils.widgets.text("tripMonthView","tripMonthView")
dbutils.widgets.text("medallion","89D227B655E5C82AECF13C3F540D4CF4")
dbutils.widgets.text("month","1")
filePath = dbutils.widgets.get("filePath")
tripMonthView = dbutils.widgets.get("tripMonthView")
med = dbutils.widgets.get("medallion")
month = dbutils.widgets.get("month")

# COMMAND ----------

# DBTITLE 1,Create Trip Data Frame
#read trip data and apply schema
from pyspark.sql.types import *

def createTripDataSet(filePath):
  tripSchema = (StructType([
    StructField("medallion", StringType(), True),
    StructField("hack_license", StringType(), True),
    StructField("vendor_id", StringType(), True),
    StructField("rate_code_id", StringType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_time_in_sec", FloatType(), True),
    StructField("trip_distance", FloatType(), True),
    StructField("pickup_long", StringType(), True),
    StructField("pickup_lat", StringType(), True),
    StructField("dropoff_long", StringType(), True),
    StructField("dropoff_lat", StringType(), True)
  ])
  )
  df = spark.read.schema(tripSchema).option("inferSchema","true").option("header","true").csv(filePath)
  return df

# COMMAND ----------

retVal = {}
def getTripCount(tripDF):
  totalCount = tripDF.count()
  retVal["total_rec"] = totalCount
  return totalCount

# COMMAND ----------

df = createTripDataSet(filePath)
display(df)

# COMMAND ----------

print(getTripCount(df))

# COMMAND ----------

# DBTITLE 1,Function to add Trip Month Column to the Data Frame
from pyspark.sql.functions import *
def addTripMonthColumn(tripDF):
  tripDF = tripDF.withColumn("pickupMonth",month(date_format(unix_timestamp(tripDF["pickup_datetime"], "yyyy-MM-dd HH:mm:ss").cast("timestamp"), "yyyy-MM-dd"))) 
  return tripDF

# COMMAND ----------

df2 = addTripMonthColumn(df)
#df2.write.saveAsTable(name="tripwithMonth",mode="overwrite")
df2.select("medallion","pickupMonth").createOrReplaceGlobalTempView(tripMonthView)

# COMMAND ----------

# DBTITLE 1,Get Total Passengers for Driver in Specified Month 
def getTotalPassengersTransPortedByDriverInMonth(df,med,month):
  print(df.count())
  print(med)
  print(month)
  totalPass = df.groupBy("medallion","pickupMonth").agg(sum("passenger_count").alias("tot_pass")).where((col("medallion") == med) & (col("pickupMonth") == 1)).select("tot_pass").collect()
  print(totalPass)
  totalPassVal = totalPass[0]['tot_pass']
  print(totalPassVal)
  retVal["totalPass"] = totalPassVal

# COMMAND ----------

getTotalPassengersTransPortedByDriverInMonth(df2,med,month)

# COMMAND ----------

import json
print(retVal)
dbutils.notebook.exit(json.dumps(retVal))
