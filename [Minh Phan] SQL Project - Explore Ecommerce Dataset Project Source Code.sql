-- [Minh Phan] Explore Big Query Ecommerce Dataset by using SQL  to collect, organize and connect data from seperate worksheets to calculate percisely information for different reports.  

-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL

SELECT
    EXTRACT (MONTH FROM PARSE_DATE("%Y%m%d", date)) AS month,   
    COUNT (fullVisitorId) AS total_visit,
    COUNT (totals.pageviews) AS total_pageviews,
    COUNT (totals.transactions) AS total_transactions,
    SUM (totals.TransactionRevenue)/10000000 AS total_revenue    
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE date BETWEEN '20170101' AND '20170331'
GROUP BY month    
ORDER BY month;   

-- Query 02: Bounce rate per traffic source in July 2017.

SELECT 
    trafficSource.source AS source,
    COUNT (fullVisitorId) AS total_visit,
    COUNT (totals.bounces) AS total_no_of_bounces,
    ROUND ((COUNT (totals.bounces) / COUNT (fullVisitorId)) * 100,2) AS bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY source
ORDER BY total_visit DESC ;  

-- Query 3: Revenue by traffic source by week, by month in June 2017.

WITH month_data AS(
SELECT
  "Month" AS time_type,
  FORMAT_DATE("%Y%m", PARSE_DATE("%Y%m%d", date)) AS month,
  trafficSource.source AS source,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
GROUP BY 1,2,3
ORDER BY Revenue DESC
),

week_data AS(
SELECT
  "Week" AS time_type,
  FORMAT_DATE("%Y%W", PARSE_DATE("%Y%m%d", date)) AS date,
  trafficSource.source AS source,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
GROUP BY 1,2,3
ORDER BY revenue DESC
)

SELECT * FROM month_data
UNION ALL 
SELECT * FROM week_data

--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017.

WITH raw AS (
SELECT 
    FORMAT_DATE("%Y%m",parse_date("%Y%m%d",date)) AS month,
    SUM (CASE WHEN totals.transactions IS NULL THEN totals.pageviews END ) AS sum_pageview_non_purchase,
    SUM (CASE WHEN totals.transactions >= 1 THEN totals.pageviews END ) AS sum_pageview_purchase,
    SUM (totals.pageviews) AS sum_pageview_total,
    COUNT (DISTINCT fullVisitorId) AS total_unique_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE date BETWEEN '20170601' AND '20170731' 
GROUP BY month
)

SELECT month,
       (sum_pageview_purchase / total_unique_user) AS avg_pageview_purchase,
       (sum_pageview_non_purchase / total_unique_user) AS avg_pageview_non_purchase
FROM raw 
GROUP BY month, avg_pageview_purchase, avg_pageview_non_purchase;


-- Query 05: Average number of transactions per user that made a purchase in July 2017.

SELECT 
    FORMAT_DATE("%Y%m",parse_date("%Y%m%d",date)) AS month,
    SUM(totals.transactions)/COUNT(DISTINCT fullvisitorid) AS Avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE totals.transactions>=1
GROUP BY month;


-- Query 06: Query 06: Average amount of money spent per session. Only include purchaser data in July 2017.

SELECT
    FORMAT_DATE("%Y%m",parse_date("%Y%m%d",date)) AS month,
    ((SUM(totals.totalTransactionRevenue)/SUM(totals.visits))/POWER(10,6)) AS avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE totals.transactions is not null
GROUP BY month;

-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. 

WITH henley_user_id AS ( 
SELECT DISTINCT fullVisitorId 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
    UNNEST (hits) hits,
    UNNEST (hits.product) product
WHERE product.v2ProductName = "YouTube Men's Vintage Henley" AND product.productRevenue is not null
 )

,all_product AS (
SELECT 
      product.v2ProductName AS product, 
      SUM (productQuantity) AS total_quatity    
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
    UNNEST (hits) hits,
    UNNEST (hits.product) product      
WHERE  totals.transactions >=1 AND fullVisitorId IN (SELECT * FROM henley_user_id) AND productQuantity IS NOT NULL AND product.productRevenue is not null
GROUP BY product
)

 SELECT 
    product, 
    total_quatity
 FROM all_product
 WHERE product != "YouTube Men's Vintage Henley"
 ORDER BY total_quatity DESC; 


--Query 08: Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017. 

WITH raw AS (
SELECT
    FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d',date)) AS month,  
    SUM (CASE WHEN hits.eCommerceAction.action_type = '2' THEN 1 END) AS num_product_view,
    SUM (CASE WHEN hits.eCommerceAction.action_type = '3' THEN 1 END) AS num_addtocart, 
    SUM (CASE WHEN hits.eCommerceAction.action_type = '6' THEN 1 END) AS num_purchase   
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST (hits) hits,
      UNNEST (hits.product) as product
WHERE date BETWEEN '20170101' AND '20170331' AND hits.eCommerceAction.action_type IN ('2', '3', '6')
GROUP BY month
)

SELECT month, 
       num_product_view,
       num_addtocart,
       num_purchase, 
       ROUND (100 * num_addtocart/num_product_view,2) AS add_to_cart_rate,    
       ROUND (100 * num_purchase/num_product_view,2) AS purchase_rate
FROM raw 
GROUP BY month, num_product_view,num_addtocart,num_purchase
ORDER BY month;
