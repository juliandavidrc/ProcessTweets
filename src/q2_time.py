from typing import List, Tuple
from datetime import datetime
import pandas as pd
import pyspark
from pyspark.sql import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import emoji
import json

def q2_time(file_path: str) -> List[Tuple[str, int]]:
    
    ##Read file with json.loads()   
    data = [json.loads(line) for line in open(file_path, 'r')]
    #convert to dataframe
    df = pd.DataFrame(data)
    #Selection only the columns to process to enhance time and usage memory consumption
    dfres = df[['content','id']]
    #Creating string for each content
    text = dfres['content'].str.cat(sep='\n')
    """ Creating a list value_counts with emoji.emoji_list() function, 
        finds all emoji in string and their position. After count 'emoji' field)
    """
    out = (pd.DataFrame(emoji.emoji_list(text)).value_counts('emoji')
            .rename_axis('Smiley').rename('Count').reset_index()
            .assign(Type=lambda x: x['Smiley'].apply(emoji.demojize)))
   
    #print(list(out[['Smiley','Count']].head(10).itertuples(index=False, name=None)))
    df_ret = list(out[['Smiley','Count']].head(10).itertuples(index=False, name=None))
    return df_ret
    


if __name__ == "__main__":
    file_path = "data/farmers-protest-tweets-2021-2-4.json"    
    q2_time(file_path)