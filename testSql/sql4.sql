select
	count(*)
from
	orders,lineitem
where
	o_orderdate >= '1995/2/3'
	and o_orderdate < '1995/5/6'
    and lineitem.L_ORDERKEY=O_ORDERKEY
    and L_COMMITDATE<'1996/5/3'
