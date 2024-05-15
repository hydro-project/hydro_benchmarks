/* 
SELECT C.id, AVG(CA.price)
FROM category C, item I, closed auction CA
WHERE C.id = I.categoryId
AND I.id = CA.itemid
GROUP BY C.id;
*/