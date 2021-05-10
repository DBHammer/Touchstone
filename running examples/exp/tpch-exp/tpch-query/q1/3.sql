select
    /*+ NO_INDEX(orders) */
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
from
    customer STRAIGHT_JOIN
    orders STRAIGHT_JOIN
    lineitem
where
        c_mktsegment = 'dykvNBgNA'
  and c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate < date '1995-03-10'
  and l_shipdate > date '1998-07-23'
group by
    l_orderkey,
    o_orderdate,
    o_shippriority
order by
    revenue desc,
    o_orderdate
limit 10;
