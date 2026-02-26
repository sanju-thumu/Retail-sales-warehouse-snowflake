USE  WAREHOUSE COMPUTE_WH;
USE DATABASE SNOWFLAKE_LEARNING_DB;
create schema if not exists gold;
CREATE OR REPLACE TABLE GOLD.SALES_BY_STATE AS
SELECT
    D.state,
    SUM(f.sales_price) AS total_sales
FROM
    SILVER.FACT_SALES f
JOIN
    SILVER.CUSTOMER_DIM d ON f.customer_sk = d.customer_sk
group by d.state;
SELECT * FROM GOLD.SALES_BY_STATE LIMIT 10;
CREATE OR REPLACE TABLE GOLD.SALES_BY_YAER AS
SELECT
    YEAR(transaction_date) AS sales_year,
    SUM(sales_price) AS total_sales
FROM
    SILVER.FACT_SALES 
GROUP BY sales_year;