select
    l_suppkey,
    sum(l_extendedprice * (1 - l_discount))
from
    lineitem
where
        l_shipdate >= '#34,0,1#'
  and l_shipdate < '#34,1,1#'
group by
    l_suppkey;

