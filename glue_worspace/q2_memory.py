from pyspark.context import SparkContext
from pyspark.sql.functions import col
from awsglue.context import GlueContext
from awsglue.job import Job
import pandas as pd
import emoji
import sys
from awsglue.utils import getResolvedOptions


class GlueProcessTweets:
    def __init__(self):
        params = []
        if '--JOB_NAME' in sys.argv:
            params.append('JOB_NAME')
        args = getResolvedOptions(sys.argv, params)

        self.context = GlueContext(SparkContext.getOrCreate())
        self.job = Job(self.context)

        if 'JOB_NAME' in args:
            jobname = args['JOB_NAME']
        else:
            jobname = "test"
        self.job.init(jobname, args)

    def run(self):
        dyf = read_json(self.context, "s3://julawsglue-datasets/farmers-protest-tweets-2021-2-4.json.gz")
                
        df = dyf.toDF()
        
        df_pd = df.toPandas()
        #Selection only the columns to process to enhance time and usage memory consumption
        dfres = df_pd[['content','id']]
        #Creating string for each content
        text = dfres['content'].str.cat(sep='\n')
        """ Creating a list value_counts with emoji.emoji_list() function,
            finds all emoji in string and their position. After count 'emoji' field)
        """
        out = (pd.DataFrame(emoji.emoji_list(text)).value_counts('emoji')
                .rename_axis('Smiley').rename('Count').reset_index()
                .assign(Type=lambda x: x['Smiley'].apply(emoji.demojize)))

        print(list(out[['Smiley','Count']].head(10).itertuples(index=False, name=None)))    

        self.job.commit()        


def read_json(glue_context, path):
    dynamicframe = glue_context.create_dynamic_frame.from_options(
        connection_type='s3',
        connection_options={
            'paths': [path],
            'recurse': True
        },
        format='json'
    )
    return dynamicframe



if __name__ == '__main__':
    GlueProcessTweets().run()