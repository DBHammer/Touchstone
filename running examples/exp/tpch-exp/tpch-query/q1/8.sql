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
          and r_name = 'kQMBQPC90'
          and s_nationkey = n2.n_nationkey
          and o_orderdate between date '1993-10-04' and date '1995-10-07'
          and p_type = 'jLHdUU5k2WT8jlwHNlIS'
    ) as all_nations
group by
    o_year
order by
    o_year;
