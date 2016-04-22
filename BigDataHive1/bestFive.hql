add jar ./brickhouse-0.7.1.jar;
create temporary function collect as 'brickhouse.udf.collect.CollectUDAF';

CREATE TABLE IF NOT EXISTS bills (my_date STRING, products ARRAY<STRING>)
ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ','
	COLLECTION ITEMS TERMINATED BY '|';

LOAD DATA INPATH 'hiveInput/hiveExample.txt'
OVERWRITE INTO TABLE bills;

DROP TABLE IF EXISTS prodMonth;

CREATE TABLE prodMonth AS
SELECT prodMonthArray.my_month as my_month, product
FROM (SELECT date_format(my_date, "yyyy-MM") AS my_month, products
	FROM bills) prodMonthArray
LATERAL VIEW explode(prodMonthArray.products) prodtable AS product;

SELECT prodMonthCount.my_month as monthProd, collect(prodMonthCount.product, prodMonthCount.prodCount) prodCountMap
	FROM (SELECT my_month, product, count(1) as prodCount
	FROM prodMonth
	GROUP BY my_month, product) prodMonthCount
GROUP BY prodMonthCount.my_month;