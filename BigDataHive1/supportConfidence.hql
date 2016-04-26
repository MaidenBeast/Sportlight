add jar ./BigDataHiveSerDe1-0.0.1-SNAPSHOT.jar;

DROP TABLE IF EXISTS bills;
DROP TABLE IF EXISTS n_bills;
DROP TABLE IF EXISTS prodMonth;
DROP TABLE IF EXISTS prodCount;
DROP TABLE IF EXISTS prodCoupleCount;

CREATE TABLE IF NOT EXISTS bills (my_date STRING, products ARRAY<STRING>)
ROW FORMAT SERDE 'radeon.BillSerDe';

LOAD DATA INPATH 'hiveInput/example.txt'
OVERWRITE INTO TABLE bills;

CREATE TABLE IF NOT EXISTS n_bills AS
SELECT count(1) AS num
FROM bills;

CREATE TABLE prodMonth AS
SELECT prodMonthArray.my_month as my_month, product
FROM (SELECT date_format(my_date, "yyyy-MM") AS my_month, products
	FROM bills) prodMonthArray
LATERAL VIEW explode(prodMonthArray.products) prodtable AS product;

CREATE TABLE prodCount AS
SELECT product, count(1) AS pCount
FROM prodMonth
GROUP BY product;

CREATE TABLE prodCoupleCount AS
SELECT left.product AS cLeft, right.product AS cRight, count(1) AS cCount
FROM prodMonth left, prodMonth right
WHERE 	left.my_month = right.my_month AND
		left.product <> right.product
GROUP BY left.product, right.product;

CREATE TABLE supportConfidence AS
SELECT 	cLeft, cRight,
		(cCount/num)*100 AS support,
		(cCount/pCount)*100 AS confidence
FROM prodCoupleCount, n_bills, prodCount
WHERE prodCoupleCount.cLeft = prodCount.product;

SELECT * FROM supportConfidence;