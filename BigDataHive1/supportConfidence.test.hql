add jar ./BigDataHiveSerDe1-0.0.1-SNAPSHOT.jar;

DROP TABLE IF EXISTS bills;
DROP TABLE IF EXISTS n_bills;
DROP TABLE IF EXISTS prodEntry;
DROP TABLE IF EXISTS prodCouple;

CREATE TABLE IF NOT EXISTS bills (my_date STRING, products ARRAY<STRING>)
ROW FORMAT SERDE 'radeon.BillSerDe';

LOAD DATA INPATH 'hiveInput/example.txt'
OVERWRITE INTO TABLE bills;

CREATE TABLE IF NOT EXISTS n_bills AS
SELECT count(1) AS num
FROM bills;

CREATE TABLE prodEntry AS
SELECT my_date, product
LATERAL VIEW explode(bills.products) prodtable AS product;

CREATE TABLE prodCount AS
SELECT product, count(1) AS pCount
FROM prodMonth
GROUP BY product;

SELECT * from prodEntry;