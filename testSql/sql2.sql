select COUNT(*)
from partsupp,
     (
         select P_PARTKEY
         from part,
              supplier,
              partsupp,
              nation,
              region
         where p_size = 44
           and p_type like '%9a4BOm2Cjb5cF9x4yZrme2%'
           and p_partkey = ps_partkey
           and s_suppkey = ps_suppkey
           and s_nationkey = n_nationkey
           and n_regionkey = r_regionkey
           and r_name = 'ecUnSzY3') as psPP,
     region,
     nation,
     supplier
where partsupp.PS_PARTKEY = psPP.P_PARTKEY
  and s_suppkey = ps_suppkey
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and region.R_NAME = 'Ou2AWZS'
