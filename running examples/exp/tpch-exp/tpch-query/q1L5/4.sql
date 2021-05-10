select
	 /*+ JOIN_ORDER(orders, lineitem@subq1) */    
    o_orderpriority,
    count(*) as order_count
from
    orders
where
        o_orderdate >= '1997-06-14'
  and o_orderdate < '1997-09-13'
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
