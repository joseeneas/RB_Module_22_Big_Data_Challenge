# %% [markdown]
# # MODULE 22 - BIG DATA ANALYSIS WITH SPARK
# %%
#
# Step 00 - Environment Setup
#

import sys
import os
import shutil          as shu
import datetime
import warnings   
import findspark    
from   colorama    import Fore                       
from   pyspark     import SparkFiles  
from   pyspark.sql import SparkSession

start_time = datetime.datetime.now()
w, h       = shu.get_terminal_size()

def logStep(msg):
    l1 = len(msg)
    l2 = w - l1
    print(Fore.WHITE + str(datetime.datetime.now()) +  Fore.YELLOW + " STEP " + msg + Fore.WHITE + "-" * l2  )
    sys.stdout.flush()

logStep("00 - ENVIRONMENT PREPARATION")
warnings.filterwarnings("ignore", category=DeprecationWarning)
findspark.init()
spark = SparkSession.builder\
    .appName("BigDataChallenge") \
    .config("spark.sql.debug.maxToStringFields", 2000) \
    .config("spark.driver.memory", "8g") \
    .config("spark.sql.broadcastTimeout", -1) \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.yarn.maxAppAttempts",1000000) \
    .config("spark.sql.files.ignoreMissingFiles",True) \
    .config("spark.core.connection.ack.wait.timeout",-1) \
    .config("spark.shuffle.io.connectionTimeout",-1) \
    .config("spark.rpc.lookupTimeout ",-1) \
    .config("spark.sql.shuffle.partitions", 32) \
    .master("local[8]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

def spark_SQL_Run(step_Number, step_Message1, sql_Statement):
  start_time         = datetime.datetime.now()
  logStep(step_Number + " - " + step_Message1)
  print()
  try:
    spark.sql(sql_Statement).show(5)
  except Exception as e:
    logStep(F"{step_Number} - Exception: {e}")
    sys.exit(1)
  else:
    logStep(step_Number + " - " + "Spark SQL Statement Executed Successfully")
  logStep(step_Number + " - " + "DONE")
  end_time           = datetime.datetime.now()
  step_elapsed_time  = end_time - start_time
  logStep(F"{step_Number} - ELAPSED TIME: {step_elapsed_time} seconds")
  return step_elapsed_time

def runtime_Diff(step_Number, step_Message1,base_SQL, cached_SQL):
  start_time          = datetime.datetime.now()
  logStep(step_Number + " - RUNTIME DIFFERENCE")
  time_difference     = base_SQL - cached_SQL
  logStep(F"{step_Number} - Time required for a non-cached Query : {base_SQL}")
  logStep(F"{step_Number} - Time required for a cached/part Query: {cached_SQL}")
  logStep(F"{step_Number} - Time difference                      : {time_difference}")
  logStep(step_Number + " - DONE")
  end_time            = datetime.datetime.now()
  step_elapsed_time   = end_time - start_time
  logStep(F"{step_Number} - ELAPSED TIME: {step_elapsed_time} seconds")
  return step_elapsed_time

print(F"Copyright        : {sys.copyright}")
print(F"OS Platform      : {sys.platform}")
print(F"OS Name          : {os.name}")
print(F"OS HOME          : {os.environ.get('HOME')}")
print(F"OS uName         : {os.uname().sysname}")
print(F"OS NodeName      : {os.uname().nodename}")
print(F"OS Release       : {os.uname().release}")
print(F"OS Release Ver   : {os.uname().version}")
print(F"OS Machine       : {os.uname().machine}")
print(F"Process ID       : {os.getpid()}")
print(F"Parent Process   : {os.getppid()}")
print(F"OS User          : {os.getlogin()}")
print(F"OS User ID       : {os.getuid()}")
print(F"OS Group ID      : {os.getgid()}")
print(F"OS Effective ID  : {os.geteuid()}")
print(F"OS Effective GID : {os.getegid()}")
print(F"Current dir      : {os.getcwd()}")
print(F"Python version   : {sys.version}")
print(F"Version info     : {sys.version_info}")
print(F"Python API Ver   : {sys.api_version}")
print(F"Executable       : {sys.executable}")
print(F"Spark version    : {findspark.__version__}")
print(F"Spark Home(Find) : {findspark.find()}")
print(F"Spark Home(Env)  : {os.environ.get('SPARK_HOME')}")
print(F"Spark UI         : http://localhost:4040")
print(F"Spark submit     : {sys.argv[0]}")
print(F"Hadoop Home      : {os.environ.get('HADOOP_HOME')}")
print(F"Java Home        : {os.environ.get('JAVA_HOME')}")

logStep("00 - DONE");
end_time            = datetime.datetime.now()
step00_elapsed_time = end_time - start_time
logStep(F"00 - ELAPSED TIME: {step00_elapsed_time} seconds")

# %% [markdown]
# Step 01 - Read in the source file into a DataFrame

# %%
#
# Step 01 - Read in the source file into a DataFrame.
#

start_time = datetime.datetime.now()
logStep("01 - READ SOURCE DATA");

try:
  url   = "https://2u-data-curriculum-team.s3.amazonaws.com/dataviz-classroom/v1.2/22-big-data/home_sales_revised.csv"
  spark.sparkContext.addFile(url)
  home_df = spark.read.csv(SparkFiles.get("home_sales_revised.csv"), sep=",", header=True, ignoreLeadingWhiteSpace=True)
  home_df.show(5)
except Exception as e:
  logStep(F"Exception: {e}")
  sys.exit(1)
else:
  logStep("01 - Spark operation - Source data read successfully")

logStep("01 - DONE");
end_time           = datetime.datetime.now()
step01_elapsed_time = end_time - start_time
logStep(F"01 - ELAPSED TIME: {step01_elapsed_time} seconds")

# %% [markdown]
# Step 02 - Create a temporary view of the DataFrame.

# %%
#
# Step 02 - Create a temporary view of the DataFrame.
#

start_time         = datetime.datetime.now()
logStep("02 - CREATE A TEMPORARY VIEW")

try:
  home_df.createOrReplaceTempView("home_sales")
except Exception as e:
  logStep(F"Exception: {e}")
  sys.exit(1)
else:
  logStep("02 - Spark operation - Temporary view created successfully")

logStep("02 - DONE")
end_time           = datetime.datetime.now()
step02_elapsed_time = end_time - start_time
logStep(F"02 - ELAPSED TIME: {step02_elapsed_time} seconds")

# %% [markdown]
# Step 03 - What is the average price for a four bedroom house sold in each year rounded to two decimal places?

# %%
# 
# Step 03 - What is the average price for a four bedroom house sold in each year rounded to two decimal places?
#

step03_elapsed_time = spark_SQL_Run("03", "WHAT IS THE AVERAGE PRICE?=4BR", "SELECT YEAR(date) AS YEAR, ROUND(AVG(price),2) AS AVERAGE FROM home_sales WHERE bedrooms == 4 GROUP BY YEAR(date) ORDER BY YEAR(date) DESC")

# %% [markdown]
# Step 04 - What is the average price of a home for each year the home was built that have 3 bedrooms and 3 bathrooms rounded to two decimal places?

# %%
# 
# Step 04 - What is the average price of a home for each year the home was built that have 3 bedrooms and 3 bathrooms rounded to two decimal places?
#

step04_elapsed_time = spark_SQL_Run("04", "WHAT IS THE AVERAGE PRICE?=3BR/3B", "SELECT date_built AS YEAR , ROUND(AVG(price),2) AS AVERAGE FROM home_sales WHERE bedrooms == 3 AND bathrooms == 3 GROUP BY date_built ORDER BY date_built DESC")

# %% [markdown]
# Step 05 - What is the average price of a home for each year built that have 3 bedrooms, 3 bathrooms, with two floors, and are greater than or equal to 2,000 square feet rounded to two decimal places?

# %%
#
# Step 05 - What is the average price of a home for each year built that have 3 bedrooms, 3 bathrooms, with two floors,
# and are greater than or equal to 2,000 square feet rounded to two decimal places?
#

step05_elapsed_time = spark_SQL_Run("05", "WHAT IS THE AVERAGE PRICE?=3R/3/2B", "SELECT date_built AS YEAR, ROUND(AVG(price),2) AS AVERAGE FROM home_sales WHERE bedrooms == 3 AND bathrooms == 3 AND floors == 2 AND sqft_living >= 2000 GROUP BY date_built ORDER BY date_built DESC")

# %% [markdown]
# Step 06 - What is the "view" rating for the average price of a home, rounded to two decimal places, where the homes are greater than or equal to $350,000? Although this is a small dataset, determine the run time for this query.

# %%
# 
# Step 06 - What is the "view" rating for the average price of a home, rounded to two decimal places, where the homes are greater than or equal to $350,000? Although this is a small dataset, determine the run time for this query.
#

step06_elapsed_time = spark_SQL_Run("06", "WHAT IS THE AVERAGE PRICE?=300K", "SELECT view AS VIEW, ROUND(AVG(price),2) AS AVERAGE FROM home_sales GROUP BY view HAVING ROUND(AVG(price),2) >= 350000 ORDER BY view DESC")

# %% [markdown]
# Step 07 - Cache the the temporary table home_sales.

# %%
# 
# Step 07 - Cache the the temporary table home_sales.
#

start_time         = datetime.datetime.now()
logStep("07 - CACHE HOME DATA")

try:
  spark.sql("cache table home_sales")
except Exception as e:
  logStep(F"Exception: {e}")
  sys.exit(1) 
else:
  logStep("07 - Spark operation - Cache executed successfully")

end_time           = datetime.datetime.now()
logStep("07 - DONE")
step07_elapsed_time = end_time - start_time
logStep(F"07 - ELAPSED TIME: {step07_elapsed_time} seconds")

# %% [markdown]
# Step 08 - Check if the table is cached.

# %%
# 
# Step 08 - Check if the table is cached.
#

start_time         = datetime.datetime.now()
logStep("08 - IS THE DATA CACHED?")

try:
  if (spark.catalog.isCached("home_sales") == False):
    logStep("08 - home_sales is not cached")
  else:
    logStep("08 - home_sales is cached")
except Exception as e:
    logStep(F"Exception: {e}")
    sys.exit(1)
else:
    logStep("08 - Spark operation - Cache check executed successfully")

end_time           = datetime.datetime.now()
logStep("08 - DONE")
step08_elapsed_time = end_time - start_time
logStep(F"08 - ELAPSED TIME: {step08_elapsed_time} seconds")

# %% [markdown]
# Step 09 - Using the cached data, run the query that filters out the view ratings with average price greater than or equal to $350,000. Determine the runtime and compare it to uncached runtime.

# %%
# 
# Step 09 - Using the cached data, run the query that filters out the view ratings with average price greater than or equal to $350,000. Determine the runtime and compare it to uncached runtime.
#

step09_elapsed_time = spark_SQL_Run("09", "WHAT IS THE AVERAGE PRICE?=300K", "SELECT view AS VIEW, ROUND(AVG(price),2) AS AVERAGE FROM home_sales GROUP BY view HAVING ROUND(AVG(price),2) >= 350000 ORDER BY view DESC")

# %% [markdown]
# Step 10 - Determine the runtime and compare to the original runtime

# %%
# 
# Step 10 Determine the runtime and compare to the original runtime
#

step10_elapsed_time = runtime_Diff("10", "RUNTIME DIFFERENCE", step06_elapsed_time, step09_elapsed_time)

# %% [markdown]
# Step 11 - Partition by the "date_built" field on the formatted parquet home sales data 

# %%
# 
# Step 11 - Partition by the "date_built" field on the formatted parquet home sales data 
#

start_time         = datetime.datetime.now()
logStep("11 - FORMATTED PARQUET")

try:
  home_df.write.parquet("home_parquet", mode="overwrite",partitionBy="date_built")
  home_df.show(5)
except Exception as e:
  logStep(F"Exception: {e}")
  sys.exit(1)
else:
  logStep("11 - Spark operation - Parquet file created successfully.")

end_time           = datetime.datetime.now()
logStep("11 - DONE")
step11_elapsed_time = end_time - start_time
logStep(F"11 - ELAPSED TIME: {step11_elapsed_time} seconds")

# %% [markdown]
# Step 12 - Read the parquet formatted data.

# %%
# 
# Step 12 - Read the parquet formatted data.
#

start_time         = datetime.datetime.now()
logStep("12 - READ PARQUET")

try:
  parquet_home_df = spark.read.parquet("home_parquet")
  parquet_home_df.show(5)
except Exception as e:
  logStep(F"Exception: {e}")
  sys.exit(1)
else:
  logStep("12 - Spark operation - Parquet file read successfully")

end_time           = datetime.datetime.now()
logStep("12 - DONE")
step12_elapsed_time = end_time - start_time
logStep(F"12 - ELAPSED TIME: {step12_elapsed_time} seconds")

# %% [markdown]
# Step 13 - Create a temporary table for the parquet data.

# %%
# 
# Step 13 - Create a temporary table for the parquet data.
#

start_time         = datetime.datetime.now()
logStep("13 - CREATE PARQUET VIEW")

try:
  parquet_home_df.createOrReplaceTempView("parquet_temp_home")
  parquet_home_df.show(5)
except Exception as e:
  print(F"Exception: {e}")
  sys.exit(1)
else:
  logStep("13 - Spark operation - Parquet view created successfully")

end_time           = datetime.datetime.now()
logStep("13 - DONE")
step13_elapsed_time = end_time - start_time
logStep(F"13 - ELAPSED TIME: {step13_elapsed_time} seconds")

# %% [markdown]
# Step 14 - Run the query that filters out the view ratings with average price of greater than or equal to $350,000 with the parquet DataFrame. Round your average to two decimal places

# %%
# 
# Step 14 - Run the query that filters out the view ratings with average price of greater than or equal to $350,000 with the parquet DataFrame. Round your average to two decimal places. 
#

step14_elapsed_time = spark_SQL_Run("14", "14 - REPEAT QUERY", "SELECT view AS VIEW, ROUND(AVG(price),2) AS AVERAGE FROM parquet_temp_home GROUP BY view HAVING ROUND(AVG(price),2) >= 350000 ORDER BY view DESC")

# %% [markdown]
# Step 15 - Determine the runtime and compare to the original runtime

# %%
#
# Step 15 - Determine the runtime and compare it to the cached version.
#

step15_elapsed_time = runtime_Diff("15", "RUNTIME DIFFERENCE", step06_elapsed_time, step14_elapsed_time)

# %% [markdown]
# Step 16 - Un-cache the home_sales temporary table.

# %%
# 
# Step 16 - Un-cache the home_sales temporary table.
#

start_time         = datetime.datetime.now()
logStep("16 - UNCACHE")

try:
  spark.sql("uncache table home_sales")
except Exception as e:
  logStep(F"Exception: {e}")
  sys.exit(1)
else:
  logStep("16 - Spark operation - Uncache executed successfully")

end_time           = datetime.datetime.now()
logStep("16 - DONE")
step16_elapsed_time = end_time - start_time
logStep(F"16 - ELAPSED TIME: {step16_elapsed_time} seconds")

# %% [markdown]
# Step 17 - Check if the home_sales is no longer cached

# %%
# 
# Step 17 - Check if the home_sales is no longer cached
#
start_time         = datetime.datetime.now()
logStep("17 - CACHE CHECK")

try:
  if (spark.catalog.isCached("home_sales") == False):
      logStep("17 - home_sales is not cached")
  else:
      logStep("17 - home_sales is cached")
except Exception as e:
  logStep(F"Exception: {e}")
  sys.exit(1)
else:
  logStep("17 - Spark operation - Cache check executed successfully")

end_time           = datetime.datetime.now()
logStep("17 - DONE")
step17_elapsed_time = end_time - start_time
logStep(F"17 - ELAPSED TIME: {step17_elapsed_time} seconds")

# %%
logStep("    TOTAL RUN TIME")
logStep(F" 0: {step00_elapsed_time} seconds")
logStep(F" 1: {step01_elapsed_time} seconds")
logStep(F" 2: {step02_elapsed_time} seconds")
logStep(F" 3: {step03_elapsed_time} seconds")
logStep(F" 4: {step04_elapsed_time} seconds")
logStep(F" 5: {step05_elapsed_time} seconds")
logStep(F" 6: {step06_elapsed_time} seconds - non-cached query")
logStep(F" 7: {step07_elapsed_time} seconds")
logStep(F" 8: {step08_elapsed_time} seconds")
logStep(F" 9: {step09_elapsed_time} seconds - cached query")
logStep(F"10: {step10_elapsed_time} seconds")
logStep(F"11: {step11_elapsed_time} seconds")
logStep(F"12: {step12_elapsed_time} seconds")
logStep(F"13: {step13_elapsed_time} seconds")
logStep(F"14: {step14_elapsed_time} seconds - parquet cached query")
logStep(F"15: {step15_elapsed_time} seconds")
logStep(F"16: {step16_elapsed_time} seconds")
logStep(F"17: {step17_elapsed_time} seconds")
logStep(F"TT: {step00_elapsed_time + step01_elapsed_time + step02_elapsed_time + step03_elapsed_time + step04_elapsed_time + step05_elapsed_time + step06_elapsed_time + step07_elapsed_time + step08_elapsed_time + step09_elapsed_time + step10_elapsed_time + step11_elapsed_time + step12_elapsed_time + step13_elapsed_time + step14_elapsed_time + step15_elapsed_time + step16_elapsed_time + step17_elapsed_time} seconds")
logStep(F"09: Cached Query Reduction  : {100-(step09_elapsed_time/step06_elapsed_time*100):.2f}%")
logStep(F"14: Parquet Query Reduction : {100-(step14_elapsed_time/step06_elapsed_time*100):.2f}%")
logStep("    END OF PROGRAM")


