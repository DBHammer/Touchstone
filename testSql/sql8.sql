select count(*)
from
     nation as n2,
     supplier,
     lineitem,
     orders,
     customer,
     nation as n1,
     region,
     part
where  s_suppkey = l_suppkey
  and l_orderkey = o_orderkey
  and o_custkey = c_custkey
  and c_nationkey = n1.n_nationkey
  and n1.n_regionkey = r_regionkey
  and r_name = 'n'
  and o_orderdate between date '1995/7/20' and date '1997/7/23'
  and supplier.S_NATIONKEY=n2.N_NATIONKEY
  and part.P_PARTKEY=lineitem.L_PARTKEY
  and part.P_TYPE='rm3QTVAJdTzl39Uc71kYQabpi'