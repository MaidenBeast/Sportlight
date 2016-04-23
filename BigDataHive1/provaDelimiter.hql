add jar ./BigDataHiveSerDe1-0.0.1-SNAPSHOT.jar;

DROP TABLE IF EXISTS bills;

CREATE TABLE IF NOT EXISTS bills (my_date STRING, products ARRAY<STRING>)
ROW FORMAT SERDE 'radeon.BillSerDe';

LOAD DATA INPATH 'hiveInput/example.txt'
OVERWRITE INTO TABLE bills;

SELECT *
FROM bills