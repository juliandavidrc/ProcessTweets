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

    dfaux = dfcol.select(col("created_at"), col("user_id"), col("username")).groupBy("created_at", "username").count()
    #df.sort(df["count"].desc()).show(10)   
    
    #Convert to pandas df sorted   
    df = dfaux.toPandas().sort_values("count", ascending=False)    

    #Printing tuples as datetime.date format
    df_ret = list(df[['created_at','username']].head(10).itertuples(index=False, name=None))
    print(df_ret)
    #return df_ret

if __name__ == "__main__":
    file_path = "data/farmers-protest-tweets-2021-2-4.json.gz"
    q1_memory(file_path)