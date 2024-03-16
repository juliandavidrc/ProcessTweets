from typing import List, Tuple
from datetime import datetime
import pandas as pd
import pyspark
from pyspark.sql import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    
    #Creating SparkSession 
    spark = SparkSession.builder.appName('readJson').getOrCreate()
    #Read file as pyspark object()   
    data = spark.read.json(file_path)            
    #Selection only the columns to process to enhance time and usage memory consumption
    dfaux = data.select(col("content"))
    dfres = dfaux.toPandas() 
    """ Extract @username from content field, generate value_counts
        and replace @ symbol to retrieve username.
    """
    df = (dfres['content'].str.extractall(r'(\@\w+)')[0].value_counts()
                        .rename_axis('username').reset_index())        
    df['username']=[sub.replace('@', '') for sub in df['username']]
    
    print(list(df.head(10).itertuples(index=False, name=None)))
    
if __name__ == "__main__":
    file_path = "data/farmers-protest-tweets-2021-2-4.json.gz"
    q3_memory(file_path)