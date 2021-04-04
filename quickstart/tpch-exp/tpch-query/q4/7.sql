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
             (n1.n_name = '5gCj52nPc6r3W'
           and n2.n_name = 'pSuHtRq7wFqlrb')
            or (n1.n_name = 'gNhUTgmBOkMK'
           and n2.n_name = 'rFpkjI1cSsEmb')
             )
           and l_shipdate between date '1996-10-09'
           and date '1998-11-21'
     ) as shipping
group by supp_nation,
         cust_nation,
         l_year
order by supp_nation,
         cust_nation,
         l_year;
