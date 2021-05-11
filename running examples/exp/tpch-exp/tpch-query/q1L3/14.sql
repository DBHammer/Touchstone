
select /*+ NO_INDEX(lineitem) */
            100.00 * sum(case
                             when p_type like 'PROMO%'
                                 then l_extendedprice * (1 - l_discount)
                             else 0
            end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
    part straight_join lineitem
where
        l_partkey = p_partkey
  and l_shipdate >= date '1992-01-24'
  and l_shipdate < date '1992-02-25';
