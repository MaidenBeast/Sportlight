add jar ./BigDataHiveSerDe1-0.0.1-SNAPSHOT.jar;

DROP TABLE IF EXISTS bills;
DROP TABLE IF EXISTS n_bills;
DROP TABLE IF EXISTS prodEntry;
DROP TABLE IF EXISTS prodCount;
DROP TABLE IF EXISTS prodCoupleCount;
DROP TABLE IF EXISTS supportConfidence;

CREATE TABLE IF NOT EXISTS bills (my_date STRING, products ARRAY<STRING>)
ROW FORMAT SERDE 'radeon.BillSerDe';

LOAD DATA INPATH 'hiveInput/example.txt'
OVERWRITE INTO TABLE bills;

CREATE TABLE IF NOT EXISTS n_bills AS
SELECT count(1) AS num
FROM bills;

CREATE TABLE prodEntry AS
SELECT my_date, product
FROM bills
LATERAL VIEW explode(bills.products) prodtable AS product;

CREATE TABLE prodCount AS
SELECT product, count(1) AS pCount
FROM prodEntry
GROUP BY product;

CREATE TABLE prodCoupleCount AS
SELECT leftPE.product AS cLeft, rightPE.product AS cRight, count(1) AS cCount
FROM prodEntry leftPE, prodEntry rightPE
WHERE 	leftPE.my_date = rightPE.my_date AND
		leftPE.product <> rightPE.product
GROUP BY leftPE.product, rightPE.product;

CREATE TABLE supportConfidence AS
SELECT 	cLeft, cRight,
		(cCount/num)*100 AS support,
		(cCount/pCount)*100 AS confidence
FROM prodCoupleCount, n_bills, prodCount
WHERE prodCoupleCount.cLeft = prodCount.product;

SELECT * FROM supportConfidence;