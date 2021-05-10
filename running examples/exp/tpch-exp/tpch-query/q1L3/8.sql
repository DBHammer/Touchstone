select
    o_year,
    sum(case
            when nation = 'UNITED STATES' then volume
            else 0
        end) / sum(volume) as mkt_share
from
    (
        select   /*+ NO_INDEX(orders)
                    NO_INDEX(lineitem) */
            extract(year from o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) as volume,
            n2.n_name as nation
        from
            region straight_join
            nation n1 straight_join
                  customer straight_join
                  orders straight_join lineitem
                  straight_join supplier
                  straight_join nation n2
                  straight_join part
        where
                p_partkey = l_partkey
          and s_suppkey = l_suppkey
          and l_orderkey = o_orderkey
          and o_custkey = c_custkey
          and c_nationkey = n1.n_nationkey
          and n1.n_regionkey = r_regionkey
          and r_name = 'ljK6y36vvy'
          and s_nationkey = n2.n_nationkey
          and o_orderdate between date '1995-07-27' and date '1997-07-29'
          and p_type = 'iFRGsx5z7W8mmTIrSPKMCj9h3'
    ) as all_nations
group by
    o_year
order by
    o_year;
