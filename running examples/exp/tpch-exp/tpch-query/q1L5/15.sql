select
    l_suppkey,
    sum(l_extendedprice * (1 - l_discount))
from
    lineitem
where
        l_shipdate >= '1996-01-26'
  and l_shipdate < '1996-04-30'
group by
    l_suppkey;

