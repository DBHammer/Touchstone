select
	/*+ NO_INDEX(orders)*/    
c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
from
    nation straight_join
    customer straight_join
         orders straight_join
         lineitem
where
        c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate >= date '#25,0,1#'
  and o_orderdate < date '#25,1,1#'
  and l_returnflag = '#26,0,0#'
  and c_nationkey = n_nationkey
group by
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
order by
    revenue desc
limit 20;
