# %% [markdown]
# # MODULE 22 - BIG DATA ANALYSIS WITH SPARK

# %% [markdown]
# Step 00 - Environment Setup

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
    print(Fore.WHITE + str(datetime.datetime.now()) +  Fore.YELLOW + ' STEP ' + msg + Fore.WHITE + '-' * l2  )
    sys.stdout.flush()
    
def printSeparator():
    print(Fore.GREEN + '-' * w + Fore.WHITE)
    
logStep('00 - ENVIRONMENT PREPARATION')


warnings.filterwarnings('ignore')

print(F'Copyright        : {sys.copyright}')
print(F'OS Platform      : {sys.platform}')
print(F'OS Name          : {os.name}')
print(F'OS HOME          : {os.environ.get("HOME")}')
print(F'OS uName         : {os.uname().sysname}')
print(F'OS NodeName      : {os.uname().nodename}')
print(F'OS Release       : {os.uname().release}')
print(F'OS Release Ver   : {os.uname().version}')
print(F'OS Machine       : {os.uname().machine}')
print(F'Process ID       : {os.getpid()}')
print(F'Parent Process   : {os.getppid()}')
print(F'OS User          : {os.getlogin()}')
print(F'OS User ID       : {os.getuid()}')
print(F'OS Group ID      : {os.getgid()}')
print(F'OS Effective ID  : {os.geteuid()}')
print(F'OS Effective GID : {os.getegid()}')
print(F'Current dir      : {os.getcwd()}')
print(F'Python version   : {sys.version}')
print(F'Version info     : {sys.version_info}')
print(F'Python API Ver   : {sys.api_version}')
print(F'Executable       : {sys.executable}')
print(F'Hadoop home      : {os.environ.get("HADOOP_HOME")}')
print(F'Spark version    : {findspark.__version__}')
print(F'Spark home(Find) : {findspark.find()}')
print(F'Spark Home(Env)  : {os.environ.get("SPARK_HOME")}')
print(F'Spark UI         : http://localhost:4040')
print(F'Spark submit     : {sys.argv[0]}')
print(F'Java home        : {os.environ.get("JAVA_HOME")}')

logStep("00 - DONE");
end_time           = datetime.datetime.now()
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
  findspark.init()
  spark = SparkSession.builder.appName("SparkSQL").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
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
  home_df.createOrReplaceTempView('home_sales')
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

start_time         = datetime.datetime.now()
logStep("03 - WHAT IS THE AVERAGE PRICE?=4BR")

try:
  spark.sql("SELECT YEAR(date)          AS YEAR, \
                    ROUND(AVG(price),2) AS AVERAGE \
               FROM home_sales \
              WHERE bedrooms == 4 \
           GROUP BY YEAR(date) \
           ORDER BY YEAR(date) DESC").show(5)
except Exception as e:
     logStep(F"Exception: {e}")
     sys.exit(1)
else:
     logStep("03 - Spark operation - Query executed successfully")

     
logStep("03 - DONE")
end_time           = datetime.datetime.now()
step03_elapsed_time = end_time - start_time
logStep(F"03 - ELAPSED TIME: {step03_elapsed_time} seconds")

# %% [markdown]
# Step 04 - What is the average price of a home for each year the home was built that have 3 bedrooms and 3 bathrooms rounded to two decimal places?

# %%
# 
# Step 04 - What is the average price of a home for each year the home was built that have 3 bedrooms and 3 bathrooms rounded to two decimal places?
#

start_time         = datetime.datetime.now()
logStep("04 - WHAT IS THE AVERAGE PRICE?=3BR/3B")

try:
  spark.sql("SELECT date_built          AS YEAR , \
                    ROUND(AVG(price),2) AS AVERAGE  \
               FROM home_sales \
              WHERE bedrooms == 3 AND \
                    bathrooms == 3 \
          GROUP BY date_built \
          ORDER BY date_built DESC").show(5)    
except Exception as e:
     logStep(F"Exception: {e}")
     sys.exit(1)
else:
     logStep("04 - Spark operation - Query executed successfully")


end_time           = datetime.datetime.now()
logStep("04 - DONE")
step04_elapsed_time = end_time - start_time
logStep(F"04 - ELAPSED TIME: {step04_elapsed_time} seconds")


# %% [markdown]
# Step 05 - What is the average price of a home for each year built that have 3 bedrooms, 3 bathrooms, with two floors, and are greater than or equal to 2,000 square feet rounded to two decimal places?

# %%
#
# Step 05 - What is the average price of a home for each year built that have 3 bedrooms, 3 bathrooms, with two floors,
# and are greater than or equal to 2,000 square feet rounded to two decimal places?
#

start_time         = datetime.datetime.now()
logStep("05 - WHAT IS THE AVERAGE PRICE?=3R/3/2")

try:
  spark.sql("SELECT date_built          AS YEAR, \
                    ROUND(AVG(price),2) AS AVERAGE \
               FROM home_sales \
              WHERE bedrooms    == 3 AND \
                    bathrooms   == 3 AND \
                    floors      == 2 AND \
                    sqft_living >= 2000 \
           GROUP BY date_built \
           ORDER BY date_built DESC").show(5)
except Exception as e:
  LogStep(F"Exception: {e}")
  sys.exit(1)
else:
  logStep("05 = Spark operation - Query executed successfully")


end_time           = datetime.datetime.now()
logStep("05 - DONE")
step05_elapsed_time = end_time - start_time
logStep(F"05 - ELAPSED TIME: {step05_elapsed_time} seconds")


# %% [markdown]
# Step 06 - What is the "view" rating for the average price of a home, rounded to two decimal places, where the homes are greater than or equal to $350,000? Although this is a small dataset, determine the run time for this query.

# %%
# 
# Ste 06 - What is the "view" rating for the average price of a home, rounded to two decimal places, where the homes are greater than or equal to $350,000? Although this is a small dataset, determine the run time for this query.
#

start_time         = datetime.datetime.now()
logStep("06 - WHAT IS THE AVERAGE PRICE?=300K")

try:
  spark.sql("SELECT view                AS VIEW, \
                    ROUND(AVG(price),2) AS AVERAGE \
               FROM home_sales \
           GROUP BY view \
             HAVING ROUND(AVG(price),2) >= 350000 \
           ORDER BY view DESC").show(5)
except Exception as e:
  logStep(F"Exception: {e}")
  sys.exit(1) 
else:
  logStep("06 - Spark operation - Query executed successfully")

end_time           = datetime.datetime.now()
logStep("06 - DONE")
step06_elapsed_time = end_time - start_time
logStep(F"06 - ELAPSED TIME: {step06_elapsed_time} seconds")


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
  if (spark.catalog.isCached('home_sales') == False):
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

start_time         = datetime.datetime.now()
logStep("09 - REPEAT QUERY")

try:
  spark.sql("SELECT view                AS VIEW, \
                    ROUND(AVG(price),2) AS AVERAGE \
               FROM home_sales \
           GROUP BY view \
             HAVING ROUND(AVG(price),2) >= 350000 \
           ORDER BY view DESC").show(5)
except Exception as e:
  logStep(F"Exception: {e}")
  sys.exit(1)
else:
  logStep("09 - Spark operation - Query executed successfully")
  
end_time           = datetime.datetime.now()
logStep("09 - DONE")
step09_elapsed_time = end_time - start_time
logStep(F"09 - ELAPSED TIME: {step09_elapsed_time} seconds")


# %% [markdown]
# Step 10 - Determine the runtime and compare to the original runtime

# %%
# 
# Step 10 Determine the runtime and compare to the original runtime
#

start_time         = datetime.datetime.now()
logStep("10 - RUNTIME DIFFERENCE")

time_difference = step06_elapsed_time - step09_elapsed_time
logStep(F"10 - Time required for a non-cached Query: {step06_elapsed_time}")
logStep(F"10 - Time required for a cached Query    : {step09_elapsed_time}")
logStep(F"10 - Time difference                     : {time_difference}")


end_time           = datetime.datetime.now()
logStep("10 - DONE")
step10_elapsed_time = end_time - start_time
logStep(F"10 - ELAPSED TIME: {step10_elapsed_time} seconds")

# %% [markdown]
# Step 11 - Partition by the "date_built" field on the formatted parquet home sales data 

# %%
# 
# Step 11 - Partition by the "date_built" field on the formatted parquet home sales data 
#

start_time         = datetime.datetime.now()
logStep("11 - FORMATTED PARQUET")

try:
  home_df.write.parquet('home_parquet', mode='overwrite',partitionBy='date_built')
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
  parquet_home_df = spark.read.parquet('home_parquet')
  parquet_home_df.show(5)
except Exception as e:
  logStep(Fore.RED + F"Exception: {e}")
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
  parquet_home_df.createOrReplaceTempView('parquet_temp_home')
  parquet_home_df.show(5)
except Exception as e:
  print(Fore.RED + F"Exception: {e}")
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

start_time         = datetime.datetime.now()
logStep("14 - REPEAT QUERY")

try:
  spark.sql("SELECT view                AS VIEW, \
                    ROUND(AVG(price),2) AS AVERAGE \
               FROM parquet_temp_home \
           GROUP BY view \
             HAVING ROUND(AVG(price),2) >= 350000 \
           ORDER BY view DESC").show(5)
except Exception as e:
  logStep(Fore.RED + F"Exception: {e}")
  sys.exit(1)
else:
  logStep("14 - Spark operation - Parquet query executed successfully.")
  
end_time           = datetime.datetime.now()
logStep("14 - DONE")
step14_elapsed_time = end_time - start_time
logStep(F"14 - ELAPSED TIME: {step14_elapsed_time} seconds")

# %% [markdown]
# Step 15 Determine the runtime and compare to the original runtime

# %%
#
# Step 15 - Determine the runtime and compare it to the cached version.
#

start_time          = datetime.datetime.now()
logStep("15 - RUNTIME DIFFERENCE")

time_difference     = step06_elapsed_time - step14_elapsed_time
logStep(F"15 - Time required for a non-cached Query    : {step06_elapsed_time}")
logStep(F"15 - Time required for a parquet cached Query: {step14_elapsed_time}")
logStep(F"15 - Time difference                         : {time_difference}")
logStep("15 - DONE")
end_time            = datetime.datetime.now()
step15_elapsed_time = end_time - start_time
logStep(F"15 - ELAPSED TIME: {step15_elapsed_time} seconds")

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
  logStep(Fore.RED + F"Exception: {e}")
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
  if (spark.catalog.isCached('home_sales') == False):
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
logStep("TOTAL RUN TIME")
logStep(F" 0:   {step00_elapsed_time} seconds")
logStep(F" 1:   {step01_elapsed_time} seconds")
logStep(F" 2:   {step02_elapsed_time} seconds")
logStep(F" 3:   {step03_elapsed_time} seconds")
logStep(F" 4:   {step04_elapsed_time} seconds")
logStep(F" 5:   {step05_elapsed_time} seconds")
logStep(F" 6:   {step06_elapsed_time} seconds - non-cached query")
logStep(F" 7:   {step07_elapsed_time} seconds")
logStep(F" 8:   {step08_elapsed_time} seconds")
logStep(F" 9:   {step09_elapsed_time} seconds - cached query")
logStep(F"10:   {step10_elapsed_time} seconds")
logStep(F"11:   {step11_elapsed_time} seconds")
logStep(F"12:   {step12_elapsed_time} seconds")
logStep(F"13:   {step13_elapsed_time} seconds")
logStep(F"14:   {step14_elapsed_time} seconds - parquet cached query")
logStep(F"15:   {step15_elapsed_time} seconds")
logStep(F"16:   {step16_elapsed_time} seconds")
logStep(F"17:   {step17_elapsed_time} seconds")
logStep(F"TT:   {step00_elapsed_time + step01_elapsed_time + step02_elapsed_time + step03_elapsed_time + step04_elapsed_time + step05_elapsed_time + step06_elapsed_time + step07_elapsed_time + step08_elapsed_time + step09_elapsed_time + step10_elapsed_time + step11_elapsed_time + step12_elapsed_time + step13_elapsed_time + step14_elapsed_time + step15_elapsed_time + step16_elapsed_time + step17_elapsed_time} seconds")
logStep(F'06 Cached Query Reduction  : {100-(step09_elapsed_time/step06_elapsed_time*100):.2f}%')
logStep(F'14 Parquet Query Reduction : {100-(step14_elapsed_time/step06_elapsed_time*100):.2f}%')
logStep("END OF PROGRAM-")


