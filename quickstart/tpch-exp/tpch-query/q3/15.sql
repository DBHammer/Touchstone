select
    l_suppkey,
    sum(l_extendedprice * (1 - l_discount))
from
    lineitem
where
        l_shipdate >= '1994-02-25'
  and l_shipdate < '1994-05-31'
group by
    l_suppkey;

