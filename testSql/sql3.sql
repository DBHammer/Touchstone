select
	count(*)
from
	customer,
	orders,
 	lineitem
where
	c_mktsegment = 'Y4XpKIoMGz'
	and c_custkey = o_custkey
 	and l_orderkey = o_orderkey
 	and o_orderdate < date '1995/3/10'
 	and l_shipdate > date '1998/7/23'