from typing import List, Tuple
from datetime import datetime
import pandas as pd
import pyspark
from pyspark.sql import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import emoji


def q2_memory(file_path: str) -> List[Tuple[str, int]]:
    #Creating SparkSession 
    spark = SparkSession.builder.appName('readJson').getOrCreate()
    #Read file as pyspark object()   
    data = spark.read.json(file_path)            
    #Selection only the columns to process to enhance time and usage memory consumption
    dfaux = data.select(col("content"), col("id"))
    df = dfaux.toPandas() 
    #Creating string for each content
    text = df['content'].str.cat(sep='\n')
    """ Creating a list value_counts with emoji.emoji_list() function, 
        finds all emoji in string and their position. After count 'emoji' field)
    """
    out = (pd.DataFrame(emoji.emoji_list(text)).value_counts('emoji')
            .rename_axis('Smiley').rename('Count').reset_index()
            .assign(Type=lambda x: x['Smiley'].apply(emoji.demojize)))
   
    print(list(out[['Smiley','Count']].head(10).itertuples(index=False, name=None)))


if __name__ == "__main__":
    file_path = "data/farmers-tweets.json.gz"
    q2_memory(file_path)