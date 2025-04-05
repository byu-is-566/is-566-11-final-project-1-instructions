
use database your_db;
use schema dbt_dev;

-- The two (rather complex) queries below will allow you to check to verify that you 
-- have successfully integrated the data from your containerized ETL service all
-- the way through to the stg_ecom__sales_orders table.

-- The first query compares the row count and proportion of populated fields in 
-- the table back in March of 2013 to the last 30 days (which should contain the 
-- data you've been generating).

WITH base AS (
  SELECT 
    CASE 
      WHEN TO_CHAR(ORDER_DATE, 'YYYY-MM') = '2013-03' THEN '2013-03'
      ELSE 'recent_30_days'
    END AS order_month,
    COUNT(*) AS total_rows,
    COUNT(SALES_ORDER_ID) AS SALES_ORDER_ID,
    COUNT(CUSTOMER_ID) AS CUSTOMER_ID,
    COUNT(ACCOUNT_NUMBER) AS ACCOUNT_NUMBER,
    COUNT(BILL_TO_ADDRESS_ID) AS BILL_TO_ADDRESS_ID,
    COUNT(COMMENT) AS COMMENT,
    COUNT(CREDIT_CARD_APPROVAL_CODE) AS CREDIT_CARD_APPROVAL_CODE,
    COUNT(CREDIT_CARD_ID) AS CREDIT_CARD_ID,
    COUNT(CURRENCY_RATE_ID) AS CURRENCY_RATE_ID,
    COUNT(DELIVERY_ESTIMATE_DAYS) AS DELIVERY_ESTIMATE_DAYS,
    COUNT(DUE_DATE) AS DUE_DATE,
    COUNT(FREIGHT) AS FREIGHT,
    COUNT(MODIFIED_DATE) AS MODIFIED_DATE,
    COUNT(ONLINE_ORDER_FLAG) AS ONLINE_ORDER_FLAG,
    COUNT(ORDER_DATE) AS ORDER_DATE,
    COUNT(ORDER_DETAILS) AS ORDER_DETAILS,
    COUNT(PURCHASE_ORDER_NUMBER) AS PURCHASE_ORDER_NUMBER,
    COUNT(REVISION_NUMBER) AS REVISION_NUMBER,
    COUNT(SALES_ORDER_NUMBER) AS SALES_ORDER_NUMBER,
    COUNT(SALES_PERSON_ID) AS SALES_PERSON_ID,
    COUNT(SHIP_DATE) AS SHIP_DATE,
    COUNT(SHIPPING_METHOD) AS SHIPPING_METHOD,
    COUNT(SHIP_TO_ADDRESS_ID) AS SHIP_TO_ADDRESS_ID,
    COUNT(STATUS) AS STATUS,
    COUNT(SUB_TOTAL) AS SUB_TOTAL,
    COUNT(TAX_AMT) AS TAX_AMT,
    COUNT(TERRITORY_ID) AS TERRITORY_ID,
    COUNT(TOTAL_DUE) AS TOTAL_DUE
  FROM stg_ecom__sales_orders
  WHERE 
    TO_CHAR(ORDER_DATE, 'YYYY-MM') = '2013-03'
    OR ORDER_DATE >= DATEADD(DAY, -30, CURRENT_DATE())
  GROUP BY 
    CASE 
      WHEN TO_CHAR(ORDER_DATE, 'YYYY-MM') = '2013-03' THEN '2013-03'
      ELSE 'recent_30_days'
    END
),
unpivoted AS (
  SELECT order_month, 'SALES_ORDER_ID' AS column_name, ROUND(SALES_ORDER_ID / total_rows, 3) AS proportion FROM base
  UNION ALL SELECT order_month, 'CUSTOMER_ID', ROUND(CUSTOMER_ID / total_rows, 3) FROM base
  UNION ALL SELECT order_month, 'ACCOUNT_NUMBER', ROUND(ACCOUNT_NUMBER / total_rows, 3) FROM base
  UNION ALL SELECT order_month, 'BILL_TO_ADDRESS_ID', ROUND(BILL_TO_ADDRESS_ID / total_rows, 3) FROM base
  UNION ALL SELECT order_month, 'COMMENT', ROUND(COMMENT / total_rows, 3) FROM base
  UNION ALL SELECT order_month, 'CREDIT_CARD_APPROVAL_CODE', ROUND(CREDIT_CARD_APPROVAL_CODE / total_rows, 3) FROM base
  UNION ALL SELECT order_month, 'CREDIT_CARD_ID', ROUND(CREDIT_CARD_ID / total_rows, 3) FROM base
  UNION ALL SELECT order_month, 'CURRENCY_RATE_ID', ROUND(CURRENCY_RATE_ID / total_rows, 3) FROM base
  UNION ALL SELECT order_month, 'DELIVERY_ESTIMATE_DAYS', ROUND(DELIVERY_ESTIMATE_DAYS / total_rows, 3) FROM base
  UNION ALL SELECT order_month, 'DUE_DATE', ROUND(DUE_DATE / total_rows, 3) FROM base
  UNION ALL SELECT order_month, 'FREIGHT', ROUND(FREIGHT / total_rows, 3) FROM base
  UNION ALL SELECT order_month, 'MODIFIED_DATE', ROUND(MODIFIED_DATE / total_rows, 3) FROM base
  UNION ALL SELECT order_month, 'ONLINE_ORDER_FLAG', ROUND(ONLINE_ORDER_FLAG / total_rows, 3) FROM base
  UNION ALL SELECT order_month, 'ORDER_DATE', ROUND(ORDER_DATE / total_rows, 3) FROM base
  UNION ALL SELECT order_month, 'ORDER_DETAILS', ROUND(ORDER_DETAILS / total_rows, 3) FROM base
  UNION ALL SELECT order_month, 'REVISION_NUMBER', ROUND(REVISION_NUMBER / total_rows, 3) FROM base
  UNION ALL SELECT order_month, 'SALES_ORDER_NUMBER', ROUND(SALES_ORDER_NUMBER / total_rows, 3) FROM base
  UNION ALL SELECT order_month, 'SHIP_DATE', ROUND(SHIP_DATE / total_rows, 3) FROM base
  UNION ALL SELECT order_month, 'SHIPPING_METHOD', ROUND(SHIPPING_METHOD / total_rows, 3) FROM base
  UNION ALL SELECT order_month, 'SHIP_TO_ADDRESS_ID', ROUND(SHIP_TO_ADDRESS_ID / total_rows, 3) FROM base
  UNION ALL SELECT order_month, 'STATUS', ROUND(STATUS / total_rows, 3) FROM base
  UNION ALL SELECT order_month, 'SUB_TOTAL', ROUND(SUB_TOTAL / total_rows, 3) FROM base
  UNION ALL SELECT order_month, 'TAX_AMT', ROUND(TAX_AMT / total_rows, 3) FROM base
  UNION ALL SELECT order_month, 'TERRITORY_ID', ROUND(TERRITORY_ID / total_rows, 3) FROM base
  UNION ALL SELECT order_month, 'TOTAL_DUE', ROUND(TOTAL_DUE / total_rows, 3) FROM base
  UNION ALL SELECT order_month, 'ROW_COUNT', CAST(total_rows AS FLOAT) FROM base
)
SELECT
  column_name,
  MAX(CASE WHEN order_month = '2013-03' THEN proportion END) AS non_null_prop_2013_03,
  MAX(CASE WHEN order_month = 'recent_30_days' THEN proportion END) AS non_null_prop_recent_30_days
FROM unpivoted
GROUP BY column_name
ORDER BY CASE WHEN column_name = 'ROW_COUNT' THEN 0 ELSE 1 END, 
  column_name;



-- The second query focuses in on the ORDER_DETAILS column with nested line items
-- and compares the number of line items as well as the proportion of each nested
-- attribute in March of 2013 to the last 30 days.

WITH exploded AS (
  SELECT 
    CASE 
      WHEN TO_CHAR(ORDER_DATE, 'YYYY-MM') = '2013-03' THEN '2013-03'
      ELSE 'recent_30_days'
    END AS order_month,
    t.SALES_ORDER_ID,
    d.value AS detail_item
  FROM stg_ecom__sales_orders t,
       LATERAL FLATTEN(input => t.ORDER_DETAILS) d
  WHERE 
    TO_CHAR(ORDER_DATE, 'YYYY-MM') = '2013-03'
    OR ORDER_DATE >= DATEADD(DAY, -30, CURRENT_DATE())
),
parsed AS (
  SELECT 
    order_month,
    detail_item:"LineTotal"::NUMBER AS LineTotal,
    detail_item:"ModifiedDate"::TIMESTAMP AS ModifiedDate,
    detail_item:"OrderQty"::NUMBER AS OrderQty,
    detail_item:"ProductID"::STRING AS ProductID,
    detail_item:"SalesOrderDetailID"::STRING AS SalesOrderDetailID,
    detail_item:"SpecialOfferID"::NUMBER AS SpecialOfferID,
    detail_item:"UnitPrice"::NUMBER AS UnitPrice,
    detail_item:"UnitPriceDiscount"::NUMBER AS UnitPriceDiscount
  FROM exploded
),
nested_counts AS (
  SELECT 
    CASE 
      WHEN TO_CHAR(ORDER_DATE, 'YYYY-MM') = '2013-03' THEN '2013-03'
      ELSE 'recent_30_days'
    END AS order_month,
    AVG(ARRAY_SIZE(ORDER_DETAILS)) AS avg_items_per_order
  FROM stg_ecom__sales_orders
  WHERE 
    TO_CHAR(ORDER_DATE, 'YYYY-MM') = '2013-03'
    OR ORDER_DATE >= DATEADD(DAY, -30, CURRENT_DATE())
  GROUP BY 1
),
field_completeness AS (
  SELECT 
    order_month,
    COUNT(*) AS total_items,
    COUNT(LineTotal) / COUNT(*) AS prop_LineTotal,
    COUNT(ModifiedDate) / COUNT(*) AS prop_ModifiedDate,
    COUNT(OrderQty) / COUNT(*) AS prop_OrderQty,
    COUNT(ProductID) / COUNT(*) AS prop_ProductID,
    COUNT(SalesOrderDetailID) / COUNT(*) AS prop_SalesOrderDetailID,
    COUNT(SpecialOfferID) / COUNT(*) AS prop_SpecialOfferID,
    COUNT(UnitPrice) / COUNT(*) AS prop_UnitPrice,
    COUNT(UnitPriceDiscount) / COUNT(*) AS prop_UnitPriceDiscount
  FROM parsed
  GROUP BY order_month
),
combined AS (
  SELECT 
    c.order_month,
    c.total_items,
    n.avg_items_per_order,
    c.prop_LineTotal,
    c.prop_ModifiedDate,
    c.prop_OrderQty,
    c.prop_ProductID,
    c.prop_SalesOrderDetailID,
    c.prop_SpecialOfferID,
    c.prop_UnitPrice,
    c.prop_UnitPriceDiscount
  FROM field_completeness c
  JOIN nested_counts n USING (order_month)
),
unpivoted AS (
  SELECT order_month, 'avg_items_per_order' AS field, ROUND(avg_items_per_order, 3) AS value FROM combined
  UNION ALL SELECT order_month, 'non_null_prop_LineTotal', ROUND(prop_LineTotal, 3) FROM combined
  UNION ALL SELECT order_month, 'non_null_prop_ModifiedDate', ROUND(prop_ModifiedDate, 3) FROM combined
  UNION ALL SELECT order_month, 'non_null_prop_OrderQty', ROUND(prop_OrderQty, 3) FROM combined
  UNION ALL SELECT order_month, 'non_null_prop_ProductID', ROUND(prop_ProductID, 3) FROM combined
  UNION ALL SELECT order_month, 'non_null_prop_SalesOrderDetailID', ROUND(prop_SalesOrderDetailID, 3) FROM combined
  UNION ALL SELECT order_month, 'non_null_prop_SpecialOfferID', ROUND(prop_SpecialOfferID, 3) FROM combined
  UNION ALL SELECT order_month, 'non_null_prop_UnitPrice', ROUND(prop_UnitPrice, 3) FROM combined
  UNION ALL SELECT order_month, 'non_null_prop_UnitPriceDiscount', ROUND(prop_UnitPriceDiscount, 3) FROM combined
  UNION ALL SELECT order_month, 'item_count' AS field, CAST(total_items AS FLOAT) AS value FROM combined
)
SELECT 
  field,
  MAX(CASE WHEN order_month = '2013-03' THEN value END) AS value_2013_03,
  MAX(CASE WHEN order_month = 'recent_30_days' THEN value END) AS value_recent_30_days
FROM unpivoted
GROUP BY field
ORDER BY 
  CASE WHEN field = 'item_count' THEN 0 ELSE 1 END,
  field;
