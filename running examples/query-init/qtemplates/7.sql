select supp_nation,
       cust_nation,
       l_year,
       sum(volume) as revenue
from (
         select /*+ NO_INDEX(lineitem)*/
                n1.n_name                          as supp_nation,
                n2.n_name                          as cust_nation,
                extract(year from l_shipdate)      as l_year,
                l_extendedprice * (1 - l_discount) as volume
         from nation n2 straight_join
               customer straight_join
               orders straight_join
               lineitem straight_join
               supplier straight_join
               nation n1
         where
             s_suppkey = l_suppkey
           and o_orderkey = l_orderkey
           and c_custkey = o_custkey
           and s_nationkey = n1.n_nationkey
           and c_nationkey = n2.n_nationkey
           and (
             (n1.n_name = '#17,0,0#'
           and n2.n_name = '#15,0,0#')
            or (n1.n_name = '#20,0,0#'
           and n2.n_name = '#18,0,0#')
             )
           and l_shipdate between date '#19,0,1#'
           and date '#19,1,1#'
     ) as shipping
group by supp_nation,
         cust_nation,
         l_year
order by supp_nation,
         cust_nation,
         l_year;