select
    l_suppkey,
    sum(l_extendedprice * (1 - l_discount))
from
    lineitem
where
        l_shipdate >= '1997-04-21'
  and l_shipdate < '1997-07-25'
group by
    l_suppkey;

