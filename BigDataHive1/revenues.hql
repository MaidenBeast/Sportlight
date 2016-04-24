add jar ./brickhouse-0.7.1.jar;
create temporary function collect as 'brickhouse.udf.collect.CollectUDAF';
create temporary function truncate_array as 'brickhouse.udf.collect.TruncateArrayUDF';
create temporary function map_filter_keys as 'brickhouse.udf.collect.MapFilterKeysUDF';

add jar ./BigDataHiveUDF1-0.0.1-SNAPSHOT.jar;
create temporary function sort_map as 'radeon.SortMapByKeyUDF';

add jar ./BigDataHiveSerDe1-0.0.1-SNAPSHOT.jar;

DROP TABLE IF EXISTS bills;
DROP TABLE IF EXISTS costs;

CREATE TABLE IF NOT EXISTS bills (my_date STRING, products ARRAY<STRING>)
ROW FORMAT SERDE 'radeon.BillSerDe';

LOAD DATA INPATH 'hiveInput/example.txt'
OVERWRITE INTO TABLE bills;

CREATE TABLE IF NOT EXISTS prices (product STRING, price INT)
ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '=';

LOAD DATA INPATH 'hiveInput/costs.properties'
OVERWRITE INTO TABLE prices;

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
	ORDER BY my_month;

CREATE TABLE prodMonthCount2 AS
SELECT 	prodMonthCount.product,
	sort_map(collect(prodMonthCount.my_month, prodMonthCount.prodCount*prices.price)) pcm
FROM prodMonthCount, prices
WHERE prodMonthCount.product = prices.product
GROUP BY prodMonthCount.product;

SELECT * FROM prodMonthCount2;