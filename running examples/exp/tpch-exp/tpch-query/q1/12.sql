select
             /*+ NO_INDEX(orders)
           NO_INDEX(lineitem)*/
    l_shipmode,
    sum(case
            when o_orderpriority = '1-URGENT'
                or o_orderpriority = '2-HIGH'
                then 1
            else 0
        end) as high_line_count,
    sum(case
            when o_orderpriority <> '1-URGENT'
                and o_orderpriority <> '2-HIGH'
                then 1
            else 0
        end) as low_line_count
from
    orders straight_join
    lineitem
where
        o_orderkey = l_orderkey
  and l_shipmode in ('aMiNH', 'PzJ')
  and l_commitdate < date '1993-11-21'
  and l_shipdate < date '1993-11-08'
  and l_receiptdate >= date '1996-07-06'
  and l_receiptdate < date '1998-05-21'
group by
    l_shipmode
order by
    l_shipmode;
