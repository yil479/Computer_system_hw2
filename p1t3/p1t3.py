#!/usr/bin/env python
# coding: utf-8

# In[1]:


import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.sql.functions import explode
from pyspark.sql.functions import lower
from operator import add


# ## Task2

# In[2]:


spark = SparkSession.builder.getOrCreate()
df = spark.read.format('xml').options(rowTag='page').load('hdfs:/enwiki_small.xml')


# In[3]:


df.printSchema()


# In[3]:


def get_titles(text):
    title_lst = []
    text_lower = text['text']['_VALUE']
    #Titles = re.findall(r'\[\[(.*?)\]\]',text_lower)
    if text_lower is not None:
        #convert text to lower cases
        Titles = re.findall(r'\[\[(.*?)\]\]',text_lower.lower())
        for title in Titles:
            if "#" not in title:
                if ":" in title:
                    if "category:" in title:
                        #strop all title start with empty
                        trim_title=title.split('|')[0].strip()
                        title_lst.append(trim_title)
                else:
                    trim_title=title.split('|')[0].strip()
                    title_lst.append(trim_title)
    return title_lst


# In[21]:


def PairLinks(links):
    return links[0], links[1]


# In[4]:


from pyspark.sql.types import ArrayType
from pyspark.sql.types import StringType
title_list = udf(lambda y: get_titles(y), ArrayType(StringType()))


# In[5]:


revision_list = title_list(col('revision'))
df2 = df.withColumn("revisions",revision_list)

df2_table = explode(df2.revisions)
article_table = df2.select(lower(col('title')),df2_table)
article_table= article_table.orderBy("title","col",ascending=True)
article_table = article_table.select(col("lower(title)").alias("article_name"), col("col").alias("links"))


# In[6]:


article_table.show()


# ## Task3

# In[13]:


#caculate contritbuions with corresponding rank for all neighbors
def Contribution(neighbor, rank):
    for links in neighbor:
        yield (links, rank / len(neighbor))


# In[15]:


#convert t2 to rdd form table with title and internal links
neighbor_article = article_table.rdd
#for each title find their neighbors
neighbor_table = lines.map(lambda links: PairLinks(links)).distinct().groupByKey().cache()


# In[ ]:


rank_table = neighbor_table.map(lambda neighbors: (neighbors[0], 1.0))


# In[16]:


for iteration in range(10):
    #contributions
    contributions = neighbor_table.join(rank_table).flatMap(lambda neighbor_rank: Contribution(neighbor_rank[1][0], neighbor_rank[1][1]))
    #ranks
    rank_table = contributions.reduceByKey(add).mapValues(lambda article_rank: article_rank * 0.85 + 0.15)


# In[ ]:


#convert rdd form back to dataframe
final_table = rank_table.toDF()


# In[ ]:


#order by title and rank
final_table= final_table.orderBy("_1","_2",ascending=True)


# In[ ]:


final_table.show()


# In[ ]:


spark.stop()


# RDD reference: https://tinyurl.com/yc48trsk
