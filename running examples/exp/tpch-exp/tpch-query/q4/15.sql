select
    l_suppkey,
    sum(l_extendedprice * (1 - l_discount))
from
    lineitem
where
        l_shipdate >= '1996-07-29'
  and l_shipdate < '1996-11-01'
group by
    l_suppkey;

