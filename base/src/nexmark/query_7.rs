/* 
SELECT bid.price, bid.itemid
FROM bid where bid.price =
(SELECT MAX(bid.price)
FROM bid [FIXEDRANGE
10 MINUTES PRECEDING]);
*/