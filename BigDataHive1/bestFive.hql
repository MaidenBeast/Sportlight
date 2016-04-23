add jar ./brickhouse-0.7.1.jar;
create temporary function collect as 'brickhouse.udf.collect.CollectUDAF';
create temporary function truncate_array as 'brickhouse.udf.collect.TruncateArrayUDF';
create temporary function map_filter_keys as 'brickhouse.udf.collect.MapFilterKeysUDF';

add jar ./BestFiveMapUDF-0.0.1-SNAPSHOT.jar;
create temporary function best_five_map as 'radeon.BestFiveMapUDF';

add jar ./BigDataHiveSerDe1-0.0.1-SNAPSHOT.jar;

DROP TABLE IF EXISTS bills;

CREATE TABLE IF NOT EXISTS bills (my_date STRING, products ARRAY<STRING>)
ROW FORMAT SERDE 'radeon.BillSerDe';

LOAD DATA INPATH 'hiveInput/example.txt'
OVERWRITE INTO TABLE bills;

DROP TABLE IF EXISTS prodMonth;
DROP TABLE IF EXISTS prodMonthCount;
DROP TABLE IF EXISTS prodMonthCount2;

CREATE TABLE prodMonth AS
SELECT prodMonthArray.my_month as my_month, product
FROM (SELECT date_format(my_date, "yyyy-MM") AS my_month, products
	FROM bills) prodMonthArray
LATERAL VIEW explode(prodMonthArray.products) prodtable AS product;

CREATE TABLE prodMonthCount AS 
SELECT my_month, product, count(1) as prodCount
	FROM prodMonth
	GROUP BY my_month, product
	ORDER BY my_month, prodCount DESC;

CREATE TABLE prodMonthCount2 AS
SELECT 	prodMonthCount.my_month as monthProd,
		collect(prodMonthCount.product, prodMonthCount.prodCount) pcm,
		truncate_array(collect(prodMonthCount.product),5) pca
	FROM prodMonthCount
GROUP BY prodMonthCount.my_month;

SELECT monthProd, best_five_map(map_filter_keys(pcm, pca))
FROM prodMonthCount2;