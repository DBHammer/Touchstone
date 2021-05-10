select
    l_suppkey,
    sum(l_extendedprice * (1 - l_discount))
from
    lineitem
where
        l_shipdate >= '1994-11-20'
  and l_shipdate < '1995-02-23'
group by
    l_suppkey;

