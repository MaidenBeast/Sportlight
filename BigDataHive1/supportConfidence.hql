add jar ./BigDataHiveSerDe1-0.0.1-SNAPSHOT.jar;

DROP TABLE IF EXISTS billInputs;
DROP TABLE IF EXISTS bills;
DROP TABLE IF EXISTS n_bills;
DROP TABLE IF EXISTS prodEntry;
DROP TABLE IF EXISTS prodCount;
DROP TABLE IF EXISTS prodCouple;
DROP TABLE IF EXISTS prodCoupleCount;
DROP TABLE IF EXISTS supportConfidence;

CREATE TABLE IF NOT EXISTS billInputs (my_date STRING, products ARRAY<STRING>)
ROW FORMAT SERDE 'radeon.BillSerDe';

LOAD DATA INPATH 'hiveInput/example.txt'
OVERWRITE INTO TABLE billInputs;

CREATE TABLE bills AS
SELECT reflect("java.util.UUID", "randomUUID") AS bill_id, my_date, products
FROM billInputs;

DROP TABLE IF EXISTS billInputs;

CREATE TABLE IF NOT EXISTS n_bills AS
SELECT count(1) AS num
FROM bills;

CREATE TABLE prodEntry AS
SELECT bill_id, product
FROM bills
LATERAL VIEW explode(bills.products) prodtable AS product;

DROP TABLE IF EXISTS bills;

CREATE TABLE prodCount AS
SELECT product, count(1) AS pCount
FROM prodEntry
GROUP BY product;

CREATE TABLE prodCouple AS
SELECT leftPE.product AS cLeft, rightPE.product AS cRight
FROM prodEntry leftPE, prodEntry rightPE
WHERE 	leftPE.bill_id = rightPE.bill_id AND
		leftPE.product <> rightPE.product;

DROP TABLE IF EXISTS prodEntry;

CREATE TABLE prodCoupleCount AS
SELECT cLeft, cRight, count(1) AS cCount
FROM prodCouple
GROUP BY cLeft, cRight;

DROP TABLE IF EXISTS prodCouple;

CREATE TABLE supportConfidence AS
SELECT 	cLeft, cRight,
		(cCount/num)*100 AS support,
		(cCount/pCount)*100 AS confidence
FROM prodCoupleCount, n_bills, prodCount
WHERE prodCoupleCount.cLeft = prodCount.product;

SELECT * FROM supportConfidence;