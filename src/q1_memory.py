from typing import List, Tuple
from datetime import datetime
import pandas as pd
import pyspark
from pyspark.sql import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    
    #Creating SparkSession 
    spark = SparkSession.builder.appName('readJson').getOrCreate()
    #Read file as pyspark object()   
    data = spark.read.json(file_path)    
    #transformation and renaming columns steps
    dfcol = data.withColumn("created_at", data["date"].cast('date'))\
                                .withColumn("user_id", data["user.id"])\
                                .withColumn("username", data["user.username"])

    df = dfcol.select(col("created_at"), col("user_id"), col("username")).groupBy("created_at", "username").count()
    df.sort(df["count"].desc()).show(10)    
    return df