DROP TABLE IF EXISTS Revenue;
CREATE TABLE Revenue(id SERIAL, amount DECIMAL);
INSERT INTO Revenue(amount) VALUES (5), (15), (7), (10);

DROP TABLE IF EXISTS Monthly_Return;
CREATE TABLE IF NOT EXISTS Monthly_Return(revenue DECIMAL, date DATE);
INSERT INTO Monthly_Return VALUES
    (50, '2019-12-01'),
    (50, NOW() - INTERVAL '2 week'),
    (65, NOW() - INTERVAL '2 week'), (82, '2019-11-01');