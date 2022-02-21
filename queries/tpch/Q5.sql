SELECT c_name, o_orderkey, o_orderdate FROM hyper.customer
INNER JOIN hyper.orders ON c_custkey = o_custkey
WHERE c_custkey < 3
ORDER BY c_name, o_orderkey
