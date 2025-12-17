CREATE TABLE ma
(
    `time` DateTime64(3),
    `ts` Int64,
    `symbol` String,
    `price` Decimal(18, 6)
)
ENGINE = MergeTree
PARTITION BY toDate(time)
PRIMARY KEY (time)
ORDER BY (time)
SETTINGS index_granularity = 8192

CREATE TABLE ma_oscilator
(
    `time` DateTime64(3),
    `oscilator` Decimal(18, 6)
)
ENGINE = MergeTree
PARTITION BY toDate(time)
PRIMARY KEY (time)
ORDER BY (time)
SETTINGS index_granularity = 8192

CREATE TABLE macd
(
    `date` Date,
    `time` DateTime64(3),
    `ts` Int64,
    `symbol` String,
    `macd` Decimal(18, 12)
)
ENGINE = MergeTree
PARTITION BY (date)
PRIMARY KEY (time)
ORDER BY (time)
SETTINGS index_granularity = 8192





-- SELECT * FROM ma order by time DESC LIMIT 100;
-- select count(*) from ma;
-- delete from ma where ts > 1;

-- select t1.symbol, t1.max_time, t3.max_time, ma1.price, ma3.price
-- from (
--     select symbol, max(time) as max_time
--     from ma
--     group by symbol
-- ) t1
-- inner join ma ma1 on t1.symbol = ma1.symbol and t1.max_time = ma1.time
-- inner join (
--     select t2.symbol, max(ma2.time) as max_time
--     from (
--         select symbol, max(time) as max_time
--         from ma
--         group by symbol
--     ) t2
--     inner join ma ma2 on t2.symbol = ma2.symbol
--     where t2.max_time > ma2.time
--     group by t2.symbol
-- ) t3 on t1.symbol = t3.symbol
-- inner join ma ma3 on t3.symbol = ma3.symbol and t3.max_time = ma3.time
-- ;

-- select max(t1.max_time) as time, avg((ma1.price - ma3.price)/ma3.price) as oscilator
-- from (
--     select symbol, max(time) as max_time
--     from ma
--     group by symbol
-- ) t1
-- inner join ma ma1 on t1.symbol = ma1.symbol and t1.max_time = ma1.time
-- inner join (
--     select t2.symbol, max(ma2.time) as max_time
--     from (
--         select symbol, max(time) as max_time
--         from ma
--         group by symbol
--     ) t2
--     inner join ma ma2 on t2.symbol = ma2.symbol
--     where t2.max_time > ma2.time
--     group by t2.symbol
-- ) t3 on t1.symbol = t3.symbol
-- inner join ma ma3 on t3.symbol = ma3.symbol and t3.max_time = ma3.time
-- ;

