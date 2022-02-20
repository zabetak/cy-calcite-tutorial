SELECT o_custkey, COUNT(*)
FROM lucene.orders
GROUP BY o_custkey