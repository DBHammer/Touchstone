select
    l_suppkey,
    sum(l_extendedprice * (1 - l_discount))
from
    lineitem
where
        l_shipdate >= '1997-06-15'
  and l_shipdate < '1997-09-18'
group by
    l_suppkey;

