select
	 /*+ JOIN_ORDER(orders, lineitem@subq1) */    
    o_orderpriority,
    count(*) as order_count
from
    orders
where
        o_orderdate >= '1993-09-10'
  and o_orderdate < '1993-12-10'
  and exists (
        select
            *
        from
            lineitem
        where
                l_orderkey = o_orderkey
          and l_commitdate < '1996-05-03'
    )
group by
    o_orderpriority
order by
    o_orderpriority;
