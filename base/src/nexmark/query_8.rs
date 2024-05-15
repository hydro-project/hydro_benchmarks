/* 
SELECT person.id, person.name
FROM person [RANGE 12 HOURS PRECEDING],
open auction [RANGE 12 HOURS PRECEDING]
WHERE person.id = open auction.sellerId;
*/