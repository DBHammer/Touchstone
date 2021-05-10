select
    l_suppkey,
    sum(l_extendedprice * (1 - l_discount))
from
    lineitem
where
        l_shipdate >= '1995-11-12'
  and l_shipdate < '1996-02-15'
group by
    l_suppkey;

