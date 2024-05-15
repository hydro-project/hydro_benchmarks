/* 
SELECT person.name, person.city,
person.state, open auction.id
FROM open auction, person, item
WHERE open auction.sellerId = person.id
AND person.state = ‘OR’
AND open auction.itemid = item.id
AND item.categoryId = 10;
*/