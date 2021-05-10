select
	 /*+ JOIN_ORDER(orders, lineitem@subq1) */    
    o_orderpriority,
    count(*) as order_count
from
    orders
where
        o_orderdate >= date '1997-05-03'
  and o_orderdate < date '1997-08-03'
  and exists (
        select
            *
        from
            lineitem
        where
                l_orderkey = o_orderkey
          and l_commitdate < date '1996-05-03'
    )
group by
    o_orderpriority
order by
    o_orderpriority;
