/* 
SELECT AVG(CA.price), CA.sellerId
FROM closed auction CA
[PARTITION BY CA.sellerId
ROWS 10 PRECEDING];
*/