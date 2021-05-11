select
	 /*+ JOIN_ORDER(orders, lineitem@subq1) */    
    o_orderpriority,
    count(*) as order_count
from
    orders
where
        o_orderdate >= date '#8,0,1#'
  and o_orderdate < date '#8,1,1#'
  and exists (
        select
            *
        from
            lineitem
        where
                l_orderkey = o_orderkey
          and l_commitdate < date '#9,0,1#'
    )
group by
    o_orderpriority
order by
    o_orderpriority;
