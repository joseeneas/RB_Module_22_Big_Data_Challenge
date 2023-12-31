# MODULE 22 - BIG DATA ANALYSIS WITH SPARK

In this challenge, you'll use your knowledge of SparkSQL to determine key metrics about home sales data. Then you'll use Spark to create temporary views, partition the data, cache and un-cache a temporary table, and verify that the table has been uncached.

## Project Contents

- Colab (**Google Colaboratory Notebook and Python Script**)
  - Home_Sales_Analysis_Colab.ipynb
  - home_sales_analysis_colab.py
- Output (**Results of the execution of the Python Script**)
  - Home_Sales_Analysis.txt
- home_parquet (**Parquet files generated by the Python Script**)
  - home_sales.parquet
  - home_sales_partitioned.parquet (Various partitions)
- Resource (**Local copy of the Data used in the Python Script**)
  - home_sales_revised.csv
- README.md (**This file**)
- Home_Sales_Analysis.ipynb (**Jupyter Notebook**)
- Home_Sales_Analysis.py (**Python Script**)

## Instructions

### Step by Step

#### 0. Import the necessary PySpark SQL functions for this assignment

#### 1. Read the home_sales_revised.csv data in the starter code into a Spark DataFrame

#### 2. Create a temporary table called home_sales

#### 3. What is the avg. price for a 4 bedroom house sold for each year? Round off your answer to two decimal places

#### 4. What is the avg. price of a home for each year it was built that has 3 bedrooms and 3 bathrooms? Round to 2 dec. places

#### 5. What is the avg. price of a home for each year that has 3 beds, 3 baths, 2 floors, and is >= 2,000 sq. ft? (2 dec. places)

#### 6. What is the "view" rating for homes costing >= $350,000? Determine the run time for this query, and round to 2 decimal places

#### 7. Cache your temporary table home_sales

#### 8. Check if your temporary table is cached

#### 9. Using the cached data, run the query that filters out the view ratings with an average price >= $350,000

#### 10. Determine the runtime and compare it to uncached runtime

#### 11. Partition by the "date_built" field on the formatted parquet home sales data

#### 12. Read the parquet formatted data

#### 13. Create a temporary table for the parquet data

#### 14. Run the query that filters the view ratings with an average price >= $350,000

#### 15.  Determine the runtime and compare it to uncached runtime

#### 16. Un-cache the home_sales temporary table

#### 17. Verify that the home_sales temporary table is uncached

---------------------------------------------------------------
