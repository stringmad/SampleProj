from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import lower, col,trim,split,regexp_replace,expr,avg,round
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType,IntegerType
from pyspark.sql.functions import concat_ws
from pyspark.sql.functions import concat, col, lit
from pyspark.sql.utils import AnalysisException
import sys
pattern="PT";
pattern1="[0-9]H"



#####Main function to call rest of the functions; we get hdfs input directory and hdfs output directory from spark submit jobs which are used as argument in the spark application

def main():

 try:
  sc = SparkContext.getOrCreate();
  spark = SparkSession(sc)
  log4jLogger = sc._jvm.org.apache.log4j 
  log = log4jLogger.LogManager.getLogger(__name__)
  inputpath =sys.argv[1]
  outputpath=sys.argv[2]
  data = extract_data(spark,log,inputpath)
  data_transformed = transform_data(data,log)
  load_data(data_transformed,log,outputpath)
  spark.stop()
 except Exception as e:
  print(e)


## Extract data from and create a dataframe from json files. Filter only the beef recipes 

def extract_data(spark,log,inputpath):
 try:
  #path = "/user/debarati_ray_ms/random/*"
  FreshDF = spark.read.json(inputpath).select("name","ingredients","url","image","cookTime","recipeYield","datePublished","prepTime","description")
  #Extract only recipes that have beef as one of the ingredients.
  beefDF=FreshDF.where(lower(col('ingredients')).like("%beef%"))
  log.warn('Data extracted from the input directory')
  return beefDF
 except Exception as e:
  print(e)
   
  
## Transform cooktime and preptime in minutes; add it and caluculate difficulty level; aggregate the based on diffcult level and calculate the averate cooking time


def transform_data(beefDF,log):
 try:
  cookdf=beefDF.withColumn("cookTime1", regexp_replace(col('cookTime'),pattern, ''))\
  .withColumn("cookTime_hr",expr("case when cookTime1 like'%H%' then cookTime1 else null end"))\
  .withColumn("cookTime_min",expr("case when cookTime1 like'%M%' then cookTime1 else null end"))\
  .withColumn('hour', split(col('cookTime_hr'), 'H').getItem(0))\
  .withColumn('min', split(col('cookTime_min'), 'M').getItem(0))\
  .withColumn("min1", regexp_replace(col('min'),pattern1, ''))\
  .na.fill("0","min1")\
  .na.fill("0","hour")\
  .withColumn("minutes", col('min1').cast(IntegerType()))\
  .withColumn("Hours", col('hour').cast(IntegerType()))\
  .withColumn("cookTime_total", col('Hours')*60+col('minutes'))\
  .select("name","ingredients","url","image","recipeYield","datePublished","prepTime","description","cookTime","cookTime_total")
  prepdf=cookdf.withColumn("prepTime1", regexp_replace(col('prepTime'),pattern, ''))\
  .withColumn("prepTime_hr",expr("case when prepTime1 like'%H%' then prepTime1 else null end"))\
  .withColumn("prepTime_min",expr("case when prepTime1 like'%M%' then prepTime1 else null end"))\
  .withColumn('hour', split(col('prepTime_hr'), 'H').getItem(0))\
  .withColumn('min', split(col('prepTime_min'), 'M').getItem(0))\
  .withColumn("min1", regexp_replace(col('min'),pattern1, ''))\
  .na.fill("0","min1")\
  .na.fill("0","hour")\
  .withColumn("minutes", col('min1').cast(IntegerType()))\
  .withColumn("Hours", col('hour').cast(IntegerType()))\
  .withColumn("prepTime_total", col('Hours')*60+col('minutes'))\
  .withColumn("total_cook_time", col('prepTime_total')+col('cookTime_total'))\
  .select("name","ingredients","url","image","recipeYield","datePublished","prepTime","description","cookTime","cookTime_total","prepTime_total","total_cook_time")
  classdf=prepdf.withColumn("difficulty",expr("case when total_cook_time<30 then 'easy' when total_cook_time >=30 and  total_cook_time<=60 then 'medium'  when total_cook_time >60  then 'hard'    else null end"))\
  .select("name","cookTime","cookTime_total","prepTime","prepTime_total","total_cook_time","difficulty")
    
  finaldf=classdf.groupBy("difficulty").agg(avg('total_cook_time').alias("avg_time"))\
  .withColumn("avg_total_cooking_time", round(col('avg_time'),2)).\
  select("difficulty","avg_total_cooking_time")
  log.warn('Extracted data is transformed for final loading')
  return finaldf
 except Exception as e:
  print(e)
   
#### Load the data to hdfs outputpath

def load_data(finaldf,log,outputpath):
 try:
  finaldf.write.option("header",True).\
  mode("overwrite").\
  csv(outputpath)
  log.warn('Data loaded in the output path in hdfs')
 except Exception as e:
  print(e)


###Calling main function

if __name__ == '__main__':
    main()