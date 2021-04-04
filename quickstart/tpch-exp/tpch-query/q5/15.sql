select
    l_suppkey,
    sum(l_extendedprice * (1 - l_discount))
from
    lineitem
where
        l_shipdate >= '1994-08-27'
  and l_shipdate < '1994-11-30'
group by
    l_suppkey;

