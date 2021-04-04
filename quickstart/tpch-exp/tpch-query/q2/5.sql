select /*+ NO_INDEX(orders)*/
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
from
    region straight_join
    nation straight_join
         customer straight_join
         orders straight_join
         lineitem straight_join
         supplier
where
        c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and l_suppkey = s_suppkey
  and c_nationkey = s_nationkey
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = '7SZia49EtI'
  and o_orderdate >= date '1992-05-15'
  and o_orderdate < date '1993-05-17'
group by
    n_name
order by
    revenue desc;