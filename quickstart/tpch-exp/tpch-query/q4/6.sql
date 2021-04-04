select
    sum(l_extendedprice * l_discount) as revenue
from
    lineitem
where
        l_shipdate >= date '1996-06-07'
  and l_shipdate < date '1998-11-18'
  and l_discount between 0.002609050273895264 and  0.029303738474845888
  and l_quantity < 14.080395758152008;
