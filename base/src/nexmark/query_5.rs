/* Original nexmark query
SELECT bid.itemid
FROM bid [RANGE 60 MINUTES PRECEDING]
WHERE (SELECT COUNT(bid.itemid)
FROM bid [PARTITION BY bid.itemid
RANGE 60 MINUTES PRECEDING])
>= ALL (SELECT COUNT(bid.itemid)
FROM bid [PARTITION BY bid.itemid
RANGE 60 MINUTES PRECEDING];
*/

/* Apache Flink query 
-- -------------------------------------------------------------------------------------------------
-- Query 5: Hot Items
-- -------------------------------------------------------------------------------------------------
-- Which auctions have seen the most bids in the last period?
-- Illustrates sliding windows and combiners.
--
-- The original Nexmark Query5 calculate the hot items in the last hour (updated every minute).
-- To make things a bit more dynamic and easier to test we use much shorter windows,
-- i.e. in the last 10 seconds and update every 2 seconds.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE nexmark_q5 (
  auction  BIGINT,
  num  BIGINT
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q5
SELECT AuctionBids.auction, AuctionBids.num
 FROM (
   SELECT
     auction,
     count(*) AS num,
     window_start AS starttime,
     window_end AS endtime
     FROM TABLE(
             HOP(TABLE bid, DESCRIPTOR(dateTime), INTERVAL '2' SECOND, INTERVAL '10' SECOND))
     GROUP BY auction, window_start, window_end
 ) AS AuctionBids
 JOIN (
   SELECT
     max(CountBids.num) AS maxn,
     CountBids.starttime,
     CountBids.endtime
   FROM (
     SELECT
       count(*) AS num,
       window_start AS starttime,
       window_end AS endtime
     FROM TABLE(
                HOP(TABLE bid, DESCRIPTOR(dateTime), INTERVAL '2' SECOND, INTERVAL '10' SECOND))
     GROUP BY auction, window_start, window_end
     ) AS CountBids
   GROUP BY CountBids.starttime, CountBids.endtime
 ) AS MaxBids
 ON AuctionBids.starttime = MaxBids.starttime AND
    AuctionBids.endtime = MaxBids.endtime AND
    AuctionBids.num >= MaxBids.maxn;
*/

/* 
Grizzly parameters: 10s windows every 1s, sum aggregation
*/

use chrono::{Duration, NaiveDateTime};

struct SlidingWindow<T> {
    window_size: Duration,
    slide_size: Duration,
    window: Vec<T>,
    window_start: NaiveDateTime,
    window_end: NaiveDateTime,
}

impl<T> SlidingWindow::<T> {
    fn new(window_size: Duration, slide_size: Duration) -> Self {
        SlidingWindow {
            window_size,
            slide_size,
            window: Vec::new(),
            window_start: NaiveDateTime::from_timestamp(0, 0),
            window_end: NaiveDateTime::from_timestamp(0, 0),
        }
    }


}