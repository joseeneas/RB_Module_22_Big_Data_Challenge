# %% [markdown]
# # MODULE 22 - BIG DATA ANALYSIS WITH SPARK

# %% [markdown]
# Step 0 - Environment Setup
# 

# %%
#Title: Home Sales Analysis
# Step 0 - Environment Setup
#

import sys
import os
from   colorama import Fore
import datetime

start_time = datetime.datetime.now()

def logStep(msg):
    print(Fore.WHITE + str(datetime.datetime.now()) + ' ' + Fore.YELLOW + msg + Fore.WHITE)
    sys.stdout.flush()
    
logStep('STEP 0 - ENVIRONMENT PREPARATION======================')

import warnings   
import findspark                           
from   pyspark     import SparkFiles  
from   pyspark.sql import SparkSession
warnings.filterwarnings('ignore')

print(Fore.GREEN)
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
print()

logStep("STEP 0 - DONE=========================================");
end_time           = datetime.datetime.now()
step0_elapsed_time = end_time - start_time
logStep(F"STEP 0 - ELAPSED TIME: {step0_elapsed_time} seconds=========")

# %% [markdown]
# Step 1 - Read in the source file into a DataFrame

# %% [markdown]
# 

# %%
#
# Step 1 - Read in the source file into a DataFrame.
#

start_time = datetime.datetime.now()
logStep("STEP 1 - READ SOURCE DATA=============================");

print(Fore.GREEN)
try:
  findspark.init()
  spark = SparkSession.builder.appName("SparkSQL").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  url   = "https://2u-data-curriculum-team.s3.amazonaws.com/dataviz-classroom/v1.2/22-big-data/home_sales_revised.csv"
  spark.sparkContext.addFile(url)
  home_df = spark.read.csv(SparkFiles.get("home_sales_revised.csv"), sep=",", header=True, ignoreLeadingWhiteSpace=True)
  home_df.show()
except Exception as e:
  print(Fore.RED + F"Exception: {e}")
  sys.exit(1)
else:
  print(Fore.GREEN + "Step 1 - Spark operation - Source data read successfully.")
  print()

logStep("STEP 1 - DONE=========================================");
end_time           = datetime.datetime.now()
step1_elapsed_time = end_time - start_time
logStep(F"STEP 1 - ELAPSED TIME: {step1_elapsed_time} seconds=========")

# %% [markdown]
# Step 2 - Create a temporary view of the DataFrame.

# %%
#
# Step 2 - Create a temporary view of the DataFrame.
#

start_time         = datetime.datetime.now()
logStep("STEP 2 - CREATE A TEMPORARY VIEW======================")

print(Fore.GREEN)
try:
  home_df.createOrReplaceTempView('home_sales')
except Exception as e:
  print(Fore.RED + F"Exception: {e}")
  sys.exit(1)
else:
  print(Fore.GREEN + "Step 2 - Spark operation - Temporary view created successfully.")
  print()

logStep("STEP 2 - DONE=========================================")
end_time           = datetime.datetime.now()
step2_elapsed_time = end_time - start_time
logStep(F"STEP 2 - ELAPSED TIME: {step2_elapsed_time} seconds=========")

# %% [markdown]
# Step 3 - What is the average price for a four bedroom house sold in each year rounded to two decimal places?
# 

# %%
# 
# Step 3 - What is the average price for a four bedroom house sold in each year rounded to two decimal places?
#

start_time         = datetime.datetime.now()
logStep("STEP 3 - WHAT IS THE AVERAGE PRICE?=4BR===============")

print(Fore.GREEN)
try:
  spark.sql("SELECT YEAR(date)          AS YEAR, \
                    ROUND(AVG(price),2) AS AVERAGE \
               FROM home_sales \
              WHERE bedrooms == 4 \
           GROUP BY YEAR(date) \
           ORDER BY YEAR(date) DESC").show()
except Exception as e:
     print(Fore.RED + F"Exception: {e}")
     sys.exit(1)
else:
     print(Fore.GREEN + "Step 3 - Spark operation - Query executed successfully.")
     print()
     
logStep("STEP 3 - DONE=========================================")
end_time           = datetime.datetime.now()
step3_elapsed_time = end_time - start_time
logStep(F"STEP 3 - ELAPSED TIME: {step3_elapsed_time} seconds=========")

# %% [markdown]
# Set 4 - What is the average price of a home for each year the home was built that have 3 bedrooms and 3 bathrooms rounded to two decimal places?

# %%
# 
# 4. What is the average price of a home for each year the home was built that have 3 bedrooms and 3 bathrooms rounded to two decimal places?
#

start_time         = datetime.datetime.now()
logStep("STEP 4 - WHAT IS THE AVERAGE PRICE?=3BR/3B============")

print(Fore.GREEN)
try:
  spark.sql("SELECT date_built          AS YEAR , \
                    ROUND(AVG(price),2) AS AVERAGE  \
               FROM home_sales \
              WHERE bedrooms == 3 AND \
                    bathrooms == 3 \
          GROUP BY date_built \
          ORDER BY date_built DESC").show()    
except Exception as e:
     print(Fore.RED + F"Exception: {e}")
     sys.exit(1)
else:
     print(Fore.GREEN + "Step 4 - Spark operation - Query executed successfully.")
     print()

end_time           = datetime.datetime.now()
logStep("STEP 4 - DONE=========================================")
step4_elapsed_time = end_time - start_time
logStep(F"STEP 4 - ELAPSED TIME: {step4_elapsed_time} seconds=========")


# %% [markdown]
# Step 5 - What is the average price of a home for each year built that have 3 bedrooms, 3 bathrooms, with two floors, and are greater than or equal to 2,000 square feet rounded to two decimal places?

# %%
#
# Step 5 - What is the average price of a home for each year built that have 3 bedrooms, 3 bathrooms, with two floors,
# and are greater than or equal to 2,000 square feet rounded to two decimal places?
#

start_time         = datetime.datetime.now()
logStep("STEP 5 - WHAT IS THE AVERAGE PRICE?=3R/3/2============")

print(Fore.GREEN)
try:
  spark.sql("SELECT date_built          AS YEAR, \
                    ROUND(AVG(price),2) AS AVERAGE \
               FROM home_sales \
              WHERE bedrooms    == 3 AND \
                    bathrooms   == 3 AND \
                    floors      == 2 AND \
                    sqft_living >= 2000 \
           GROUP BY date_built \
           ORDER BY date_built DESC").show()
except Exception as e:
  print(Fore.RED + F"Exception: {e}")
  sys.exit(1)
else:
  print(Fore.GREEN + "Step 5 - Spark operation - Query executed successfully.")
  print()

end_time           = datetime.datetime.now()
logStep("STEP 5 - DONE=========================================")
step5_elapsed_time = end_time - start_time
logStep(F"STEP 5 - ELAPSED TIME: {step5_elapsed_time} seconds=========")


# %% [markdown]
# Step 6 - What is the "view" rating for the average price of a home, rounded to two decimal places, where the homes are greater than or equal to $350,000? Although this is a small dataset, determine the run time for this query.

# %%
# 
# Ste 6 - What is the "view" rating for the average price of a home, rounded to two decimal places, where the homes are greater than or equal to $350,000? Although this is a small dataset, determine the run time for this query.
#

start_time         = datetime.datetime.now()
logStep("STEP 6 - WHAT IS THE AVERAGE PRICE?=300K==============")

print(Fore.GREEN)
try:
  spark.sql("SELECT view                AS VIEW, \
                    ROUND(AVG(price),2) AS AVERAGE \
               FROM home_sales \
           GROUP BY view \
             HAVING ROUND(AVG(price),2) >= 350000 \
           ORDER BY view DESC").show()
except Exception as e:
  print(Fore.RED + F"Exception: {e}")
  sys.exit(1) 
else:
  print(Fore.GREEN + "Step 6 - Spark operation - Query executed successfully.")
  print()

end_time           = datetime.datetime.now()
logStep("STEP 6 - DONE=========================================")
step6_elapsed_time = end_time - start_time
logStep(F"STEP 6 - ELAPSED TIME: {step6_elapsed_time} seconds=========")


# %% [markdown]
# Step 7 - Cache the the temporary table home_sales.

# %%
# 
# Step 7 - Cache the the temporary table home_sales.
#

start_time         = datetime.datetime.now()
logStep("STEP 7 - CACHE HOME DATA==============================")

print(Fore.GREEN)
try:
  spark.sql("cache table home_sales")
except Exception as e:
    print(Fore.RED + F"Exception: {e}")
    sys.exit(1) 
else:
    print(Fore.GREEN + "Step 7 - Spark operation - Cache executed successfully.")
    print()
    
end_time           = datetime.datetime.now()
logStep("STEP 7 - DONE=========================================")
step7_elapsed_time = end_time - start_time
logStep(F"STEP 7 - ELAPSED TIME: {step7_elapsed_time} seconds=========")

# %% [markdown]
# Step 8 - Check if the table is cached.

# %%
# 
# Step 8 - Check if the table is cached.
#

start_time         = datetime.datetime.now()
logStep("STEP 8 - IS THE DATA CACHED===========================")

print(Fore.GREEN)
try:
  if (spark.catalog.isCached('home_sales') == False):
    print("home_sales is not cached")
  else:
    print("home_sales is cached")
except Exception as e:
    print(Fore.RED + F"Exception: {e}")
    sys.exit(1)
else:
    print(Fore.GREEN + "Step 8 - Spark operation - Cache check executed successfully.")
    print()

end_time           = datetime.datetime.now()
logStep("STEP 8 - DONE=========================================")
step8_elapsed_time = end_time - start_time
logStep(F"STEP 8 - ELAPSED TIME: {step8_elapsed_time} seconds=========")

# %% [markdown]
# Step 9 - Using the cached data, run the query that filters out the view ratings with average price greater than or equal to $350,000. Determine the runtime and compare it to uncached runtime.

# %%
# 
# Step 9 - Using the cached data, run the query that filters out the view ratings with average price greater than or equal to $350,000. Determine the runtime and compare it to uncached runtime.
#

start_time         = datetime.datetime.now()
logStep("STEP 9 - REPEAT QUERY=================================")

print(Fore.GREEN)
try:
  spark.sql("SELECT view                AS VIEW, \
                    ROUND(AVG(price),2) AS AVERAGE \
               FROM home_sales \
           GROUP BY view \
             HAVING ROUND(AVG(price),2) >= 350000 \
           ORDER BY view DESC").show()
except Exception as e:
  print(Fore.RED + F"Exception: {e}")
  sys.exit(1)
else:
  print(Fore.GREEN + "Step 9 - Spark operation - Query executed successfully.")
  print() 
  
end_time           = datetime.datetime.now()
logStep("STEP 9 - DONE=========================================")
step9_elapsed_time = end_time - start_time
logStep(F"STEP 9 - ELAPSED TIME: {step9_elapsed_time} seconds=========")


# %% [markdown]
# Step 9.1 - Determine the runtime and compare to the original runtime

# %%
# 
# Step 9.1 Determine the runtime and compare to the original runtime
#

start_time         = datetime.datetime.now()
logStep("STEP 9.1 - RUNTIME DIFFERENCE =========================")

print(Fore.GREEN)
print(F"Time required for a non-cached Query: {step6_elapsed_time}")
print(F"Time required for a cached Query    : {step9_elapsed_time}")
time_difference = step6_elapsed_time - step9_elapsed_time
print(F"Time difference                     : {time_difference}")
print(Fore.GREEN)

end_time           = datetime.datetime.now()
logStep("STEP 9.1 - DONE========================================")
step91_elapsed_time = end_time - start_time
logStep(F"STEP 9.1 - ELAPSED TIME: {step91_elapsed_time} seconds========")

# %% [markdown]
# Step 10 - Partition by the "date_built" field on the formatted parquet home sales data 
# 

# %%
# 
# Step 10 - Partition by the "date_built" field on the formatted parquet home sales data 
#

start_time         = datetime.datetime.now()
logStep("STEP 10 - FORMATTED PARQUET============================")

print(Fore.GREEN)
try:
  home_df.write.parquet('home_parquet', mode='overwrite',partitionBy='date_built')
  home_df.show()
except Exception as e:
  print(Fore.RED + F"Exception: {e}")
  sys.exit(1)
else:
  print(Fore.GREEN + "Step 10 - Spark operation - Parquet file created successfully.")
  print()   

end_time           = datetime.datetime.now()
logStep("STEP 10 - DONE=========================================")
step10_elapsed_time = end_time - start_time
logStep(F"STEP 10 - ELAPSED TIME: {step10_elapsed_time} seconds=========")

# %% [markdown]
# Step 11 - Read the parquet formatted data.

# %%
# 
# Step 11 - Read the parquet formatted data.
#

start_time         = datetime.datetime.now()
logStep("STEP 11 - READ PARQUET=================================")

print(Fore.GREEN)
try:
  parquet_home_df = spark.read.parquet('home_parquet')
  parquet_home_df.show()
except Exception as e:
  print(Fore.RED + F"Exception: {e}")
  sys.exit(1)
else:
  print(Fore.GREEN + "Step 11 - Spark operation - Parquet file read successfully.")
  print()

end_time           = datetime.datetime.now()
logStep("STEP 11 - DONE=========================================")
step11_elapsed_time = end_time - start_time
logStep(F"STEP 11 - ELAPSED TIME: {step11_elapsed_time} seconds=========")

# %% [markdown]
# Step 12 - Create a temporary table for the parquet data.

# %%
# 
# Step 12 - Create a temporary table for the parquet data.
#

start_time         = datetime.datetime.now()
logStep("STEP 12 - CREATE PARQUET VIEW==========================")

print(Fore.GREEN)
try:
  parquet_home_df.createOrReplaceTempView('parquet_temp_home')
  parquet_home_df.show()
except Exception as e:
  print(Fore.RED + F"Exception: {e}")
  sys.exit(1)
else:
  print(Fore.GREEN + "Step 12 - Spark operation - Parquet view created successfully.")
  print()

end_time           = datetime.datetime.now()
logStep("STEP 12 - DONE=========================================")
step12_elapsed_time = end_time - start_time
logStep(F"STEP 12 - ELAPSED TIME: {step12_elapsed_time} seconds=========")

# %% [markdown]
# Step 13 - Run the query that filters out the view ratings with average price of greater than or equal to $350,000 with the parquet DataFrame. Round your average to two decimal places

# %%
# 
# Step 13 - Run the query that filters out the view ratings with average price of greater than or equal to $350,000 with the parquet DataFrame. Round your average to two decimal places. 
#

start_time         = datetime.datetime.now()
logStep("STEP 13 - REPEAT QUERY=================================")

print(Fore.GREEN)
try:
  spark.sql("SELECT view                AS VIEW, \
                    ROUND(AVG(price),2) AS AVERAGE \
               FROM parquet_temp_home \
           GROUP BY view \
             HAVING ROUND(AVG(price),2) >= 350000 \
           ORDER BY view DESC").show()
except Exception as e:
  print(Fore.RED + F"Exception: {e}")
  sys.exit(1)
else:
  print(Fore.GREEN + "Step 13 - Spark operation - Parquet query executed successfully.")
  print()
  
end_time           = datetime.datetime.now()
logStep("STEP 13 - DONE=========================================")
step13_elapsed_time = end_time - start_time
logStep(F"STEP 13 - ELAPSED TIME: {step13_elapsed_time} seconds=========")

# %% [markdown]
# 
# Step 13.1 Determine the runtime and compare to the original runtime
# 

# %%
#
# Step 13.1 - Determine the runtime and compare it to the cached version.
#

start_time         = datetime.datetime.now()
logStep("STEP 13.1 - RUNTIME DIFFERENCE ========================")

print(Fore.GREEN)
print(F"Time required for a non-cached Query    : {step6_elapsed_time}")
print(F"Time required for a parquet cached Query: {step13_elapsed_time}")
time_difference = step6_elapsed_time - step13_elapsed_time
print(F"Time difference                         : {time_difference}")
print(Fore.GREEN)

end_time           = datetime.datetime.now()
logStep("STEP 13.1 - DONE=======================================")
step131_elapsed_time = end_time - start_time
logStep(F"STEP 13.1 - ELAPSED TIME: {step131_elapsed_time} seconds=======")

# %% [markdown]
# Step 14 - Un-cache the home_sales temporary table.

# %%
# 
# Step 14 - Un-cache the home_sales temporary table.
#

start_time         = datetime.datetime.now()
logStep("STEP 14 - UN-CACHE=====================================")

print(Fore.GREEN)
try:
  spark.sql("uncache table home_sales")
except Exception as e:
  print(Fore.RED + F"Exception: {e}")
  sys.exit(1)
else:
  print(Fore.GREEN + "Step 14 - Spark operation - Uncache executed successfully.")
  print()

end_time           = datetime.datetime.now()
logStep("STEP 14 - DONE=========================================")
step14_elapsed_time = end_time - start_time
logStep(F"STEP 14 - ELAPSED TIME: {step14_elapsed_time} seconds=========")

# %% [markdown]
# Step 15 - Check if the home_sales is no longer cached

# %%
# 
# Step 15 - Check if the home_sales is no longer cached
#
start_time         = datetime.datetime.now()
logStep("STEP 15 - CACHE= CHECK=================================")

print(Fore.GREEN)
try:
  if (spark.catalog.isCached('home_sales') == False):
      print("home_sales is not cached")
  else:
      print("home_sales is cached")
except Exception as e:
  print(Fore.RED + F"Exception: {e}")
  sys.exit(1)
else:
  print(Fore.GREEN + "Step 15 - Spark operation - Cache check executed successfully.")
  print()

end_time           = datetime.datetime.now()
logStep("STEP 15 - DONE=========================================")
step15_elapsed_time = end_time - start_time
logStep(F"STEP 15 - ELAPSED TIME: {step15_elapsed_time} seconds=========")


# %%
logStep("=========TOTAL RUN TIME================================")
print(Fore.GREEN)
print('Total Runtime, per Step')
print('')
print(F"Step  0:   {step0_elapsed_time} seconds")
print(F"Step  1:   {step1_elapsed_time} seconds")
print(F"Step  2:   {step2_elapsed_time} seconds")
print(F"Step  3:   {step3_elapsed_time} seconds")
print(F"Step  4:   {step4_elapsed_time} seconds")
print(F"Step  5:   {step5_elapsed_time} seconds")
print(F"{Fore.RED}Step  6:   {step6_elapsed_time} seconds - non-cached query{Fore.GREEN}")
print(F"{Fore.GREEN}Step  7:   {step7_elapsed_time} seconds")
print(F"Step  8:   {step8_elapsed_time} seconds")
print(F"{Fore.RED}Step  9:   {step9_elapsed_time} seconds - cached query")
print(F"{Fore.GREEN}Step  9.1: {step91_elapsed_time} seconds")
print(F"Step 10:   {step10_elapsed_time} seconds")
print(F"Step 11:   {step11_elapsed_time} seconds")
print(F"Step 12:   {step12_elapsed_time} seconds")
print(F"{Fore.RED}Step 13:   {step13_elapsed_time} seconds - parquet cached query")
print(F"{Fore.GREEN}Step 13.1: {step131_elapsed_time} seconds")
print(F"Step 14:   {step14_elapsed_time} seconds")
print(F"Step 15:   {step15_elapsed_time} seconds")
print(F"{Fore.YELLOW}Total  :   {step0_elapsed_time + step1_elapsed_time + step2_elapsed_time + step3_elapsed_time + step4_elapsed_time + step5_elapsed_time + step6_elapsed_time + step7_elapsed_time + step8_elapsed_time + step9_elapsed_time + step91_elapsed_time + step10_elapsed_time + step11_elapsed_time + step12_elapsed_time + step13_elapsed_time + step131_elapsed_time + step14_elapsed_time + step15_elapsed_time} seconds")
print('')
print(Fore.GREEN + 'Runtime Reduction')
print('')
print(F'{Fore.RED}Cached Query Reduction  : {100-(step9_elapsed_time/step6_elapsed_time*100):.2f}%')
print(F'Parquet Query Reduction : {100-(step13_elapsed_time/step6_elapsed_time*100):.2f}%')
print('')
logStep("=========END============================================")


