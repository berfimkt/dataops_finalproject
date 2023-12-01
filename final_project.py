#!/usr/bin/env python
# coding: utf-8

# In[4]:


import findspark
findspark.init("/opt/spark")


# In[5]:


from pyspark.sql import SparkSession, functions as F
from pyspark import SparkContext
import configparser


# #### accessKeyId,secretAccessKey'i db_conn'dan okuyacagız.

# In[6]:


config = configparser.RawConfigParser()

config.read('/dataops/db_conn')
accessKeyId = config.get('DB', 'user_name')
secretAccessKey = config.get('DB', 'password')


# In[12]:


spark = SparkSession.builder \
.appName("final project") \
.master("local[2]") \
.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0,io.delta:delta-core_2.12:2.4.0,io.delta:delta-storage:2.4.0") \
.config("spark.hadoop.fs.s3a.access.key", accessKeyId) \
.config("spark.hadoop.fs.s3a.secret.key", secretAccessKey) \
.config("spark.hadoop.fs.s3a.path.style.access", True) \
.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
.getOrCreate() 


# ## **read from minio**

# ### **- credits data**

# In[7]:


df_cred = spark.read \
.option("header", "true") \
.option("inferSchema",True) \
.load('s3a://tmdb-bronze/credits')


# In[8]:


df_cred.limit(5).show()


# In[9]:


df_cred.printSchema()


# ##### notlar:
# cast ve crew sütunları --> json structured.

# In[10]:


from pyspark.sql.functions import from_json, schema_of_json


# In[11]:


cast_schema = schema_of_json(df_cred.select("cast").limit(1).collect()[0][0])
crew_schema = schema_of_json(df_cred.select("crew").limit(1).collect()[0][0])


# In[12]:


df1_cred = df_cred.withColumn("cast", from_json(df_cred["cast"], cast_schema)) \
.withColumn("crew", from_json(df_cred["crew"], crew_schema))


# In[13]:


df1_cred.printSchema()


# In[152]:


from pyspark.sql.functions import explode, col, format_number

df_cast = df1_cred.select("movie_id", "title", explode("cast").alias("cast"), "event_time")
df_crew = df1_cred.select("movie_id", "title", explode("crew").alias("crew"), "event_time")


# In[135]:


df_cast.limit(5).show()


# In[136]:


df_cast.printSchema()


# In[137]:


df_crew.limit(5).show()


# In[138]:


df_crew.printSchema()


# #### **cast table**

# In[153]:


pre_cast = df_cast.select(
    col("movie_id"),
    col("title"),
    col("cast.cast_id").alias("cast_id"),
    col("cast.character").alias("character"),
    col("cast.credit_id").alias("credit_id"),
    col("cast.gender").alias("gender"),
    col("cast.id").alias("id"),
    col("cast.name").alias("name"))


# In[154]:


pre_cast.limit(5).show()


# In[155]:


pre_cast.printSchema()


# In[156]:


# null value check
for column in pre_cast.columns:
 col_count = pre_cast.filter( F.col(column).isNull() ).count()
 if ( col_count >= 0):
     print("{} has {} null values.".format(column, col_count))


# In[157]:


# hidden null check
for column in pre_cast.columns:
 col_count = pre_cast.filter( F.col(column) == "").count()
 if ( col_count >= 0):
     print("{} has {} null values.".format(column, col_count))


# In[164]:


cast = pre_cast


# #### **crew table**

# In[158]:


pre_crew = df_crew.select(
    col("movie_id"),
    col("title"),
    col("crew.credit_id").alias("credit_id"),
    col("crew.department").alias("department"),
    col("crew.gender").alias("gender"),
    col("crew.id").alias("id"),
    col("crew.job").alias("job"),
    col("crew.name").alias("name"))


# In[159]:


pre_crew.limit(5).show()


# In[160]:


pre_crew.printSchema()


# In[161]:


# null value check
for column in pre_crew.columns:
 col_count = pre_crew.filter( F.col(column).isNull() ).count()
 if ( col_count >= 0):
     print("{} has {} null values.".format(column, col_count))


# In[162]:


# hidden null check
for column in pre_crew.columns:
 col_count = pre_crew.filter( F.col(column) == "").count()
 if ( col_count >= 0):
     print("{} has {} null values.".format(column, col_count))


# In[163]:


crew = pre_crew


# ### **-movies data**

# In[14]:


df_movies = spark.read \
.option("header", "true") \
.option("inferSchema",True) \
.load('s3a://tmdb-bronze/movies')


# In[16]:


import pandas as pd
pd.set_option('display.max_columns', None)


# In[25]:


df_movies.printSchema()


# In[26]:


genres_schema = schema_of_json(df_movies.select("genres").limit(1).collect()[0][0])
keywords_schema = schema_of_json(df_movies.select("keywords").limit(1).collect()[0][0])
prod_comp_schema=schema_of_json(df_movies.select("production_companies").limit(1).collect()[0][0])
prod_countries_schema=schema_of_json(df_movies.select("production_countries").limit(1).collect()[0][0])
spoken_lang_schema=schema_of_json(df_movies.select("spoken_languages").limit(1).collect()[0][0])


# In[27]:


df1_movies = df_movies.withColumn("genres", from_json(df_movies["genres"], genres_schema)) \
.withColumn("keywords", from_json(df_movies["keywords"], keywords_schema)) \
.withColumn("production_companies", from_json(df_movies["production_companies"], prod_comp_schema)) \
.withColumn("production_countries", from_json(df_movies["production_countries"], prod_countries_schema)) \
.withColumn("spoken_languages", from_json(df_movies["spoken_languages"], spoken_lang_schema)) 


# In[28]:


df1_movies.printSchema()


# In[45]:


pre_movies = df1_movies.select("id", "title", "budget", "homepage", "original_language", "original_title", "overview", "popularity", "release_date", "revenue", "runtime", "status", "tagline","vote_average", "vote_count")
pre_genres = df1_movies.select("id", explode("genres").alias("genres"))
pre_keywords = df1_movies.select("id", explode("keywords").alias("keywords"))
pre_production_companies = df1_movies.select("id", explode("production_companies").alias("production_companies"))
pre_production_countries = df1_movies.select("id", explode("production_countries").alias("production_countries"))
pre_spoken_languages = df1_movies.select("id", explode("spoken_languages").alias("spoken_languages"))


# #### **movies table**

# In[46]:


pre1_movies = pre_movies.select(
    col("id").alias("movie_id"),
    col("title"),
    col("budget"),
    col("homepage"),
    col("original_language"),
    col("original_title"),
    col("overview"),
    col("popularity"),
    col("release_date"),
    col("revenue"),
    col("runtime"),
    col("status"),
    col("tagline"),
    col("vote_average"),
    col("vote_count"))


# In[48]:


pre1_movies.printSchema()


# In[63]:


from pyspark.sql.types import *

movies = pre1_movies.withColumn("movie_id", F.col("movie_id").cast(StringType())) \
.withColumn("budget", col("budget").cast(DoubleType())) \
.withColumn("popularity", col("popularity").cast(FloatType())) \
.withColumn("release_date", F.to_date(F.col("release_date"),"yyyy-MM-dd")) \
.withColumn("revenue", col("revenue").cast(DoubleType())) \
.withColumn("runtime", col("runtime").cast(IntegerType())) \
.withColumn("vote_average", col("vote_average").cast(FloatType())) \
.withColumn("vote_count", col("vote_count").cast(IntegerType()))


# In[64]:


movies.printSchema()


# #### **genres table**

# In[67]:


pre1_genres = pre_genres.select(
    col("id").alias("movie_id"),
    col("genres.id").alias("id"),
    col("genres.name").alias("name"))


# In[69]:


pre1_genres.printSchema()


# In[71]:


genres = pre1_genres.withColumn("movie_id", F.col("movie_id").cast(StringType())) \
.withColumn("id", col("id").cast(IntegerType()))


# In[74]:


genres.printSchema()


# #### **keywords table**

# In[76]:


pre1_keywords = pre_keywords.select(
    col("id").alias("movie_id"),
    col("keywords.id").alias("id"),
    col("keywords.name").alias("name"))


# In[78]:


pre1_keywords.printSchema()


# In[79]:


keywords = pre1_keywords.withColumn("movie_id", F.col("movie_id").cast(StringType())) \
.withColumn("id", col("id").cast(IntegerType()))


# In[81]:


keywords.printSchema()


# #### **production_companies table**

# In[82]:


pre1_production_companies = pre_production_companies.select(
    col("id").alias("movie_id"),
    col("production_companies.id").alias("id"),
    col("production_companies.name").alias("name"))


# In[85]:


pre1_production_companies.printSchema()


# In[86]:


production_companies = pre1_production_companies.withColumn("movie_id", F.col("movie_id").cast(StringType())) \
.withColumn("id", col("id").cast(IntegerType()))


# In[88]:


production_companies.printSchema()


# #### **production_countries table**

# In[90]:


pre1_production_countries = pre_production_countries.select(
    col("id").alias("movie_id"),
    col("production_countries.iso_3166_1").alias("iso_3166_1"),
    col("production_countries.name").alias("name"))


# In[92]:


pre1_production_countries.printSchema()


# In[94]:


production_countries = pre1_production_countries.withColumn("movie_id", F.col("movie_id").cast(StringType()))


# In[96]:


production_countries.printSchema()


# #### **spoken_languages table**

# In[98]:


pre1_spoken_languages = pre_spoken_languages.select(
    col("id").alias("movie_id"),
    col("spoken_languages.iso_639_1").alias("iso_639_1"),
    col("spoken_languages.name").alias("name"))


# In[100]:


pre1_spoken_languages.printSchema()


# In[101]:


spoken_languages = pre1_spoken_languages.withColumn("movie_id", F.col("movie_id").cast(StringType()))


# In[103]:


spoken_languages.printSchema()


# ## **write to minio in the form deltatable**

# In[186]:


table_dict = {'cast': cast, 'crew': crew, 'movies': movies, 'genres': genres, 'keywords': keywords, 'production_companies': production_companies, 'production_countries': production_countries, 'spoken_languages': spoken_languages}

for table_name, table in table_dict.items():
    deltaPath = f"s3a://tmdb-silver/{table_name}"
    
    table.write \
    .mode("overwrite") \
    .format("delta") \
    .save(deltaPath)

