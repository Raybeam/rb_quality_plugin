DROP TABLE IF EXISTS test;
CREATE TABLE test(id int, value int);
INSERT INTO test values(1,12),(2,200);

DROP TABLE IF EXISTS price;
CREATE TABLE price(id int, cost int);
INSERT INTO price values(1,2),(2,34),(3,10),(4,50);

DROP TABLE IF EXISTS rev;
CREATE TABLE rev(id int, amount DECIMAL);
INSERT INTO rev values(1,5), (2,15), (3,7), (4,10);

DROP TABLE IF EXISTS ret;
CREATE TABLE IF NOT EXISTS ret(value DECIMAL, date DATE);
INSERT INTO ret VALUES(50, '2019-12-01'), (50, NOW() - INTERVAL '2 week'), (65, NOW() - INTERVAL '2 week'), (82, '2019-11-01');