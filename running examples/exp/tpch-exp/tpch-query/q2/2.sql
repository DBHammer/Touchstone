select
s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
from region straight_join nation
         straight_join supplier
         straight_join partsupp
         straight_join part
where
        p_partkey = ps_partkey
  and s_suppkey = ps_suppkey
  and p_size = 15
  and p_type like '%7QJEu21tcyxTID%'
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'SJ'
  and ps_supplycost = (
    select
        min(ps_supplycost)
    from
    partsupp straight_join
    supplier straight_join
    nation straight_join
    region
    where
            p_partkey = ps_partkey
      and s_suppkey = ps_suppkey
      and s_nationkey = n_nationkey
      and n_regionkey = r_regionkey
      and r_name = '9'
)
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey
limit 100;
