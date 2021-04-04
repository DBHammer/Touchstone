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
             (n1.n_name = 'UUluPiAIUNVq'
           and n2.n_name = 'fn1')
            or (n1.n_name = 'Pdryl7WtNL'
           and n2.n_name = '5X0CZAIMrH0NRY')
             )
           and l_shipdate between date '1992-07-06'
           and date '1994-08-18'
     ) as shipping
group by supp_nation,
         cust_nation,
         l_year
order by supp_nation,
         cust_nation,
         l_year;
