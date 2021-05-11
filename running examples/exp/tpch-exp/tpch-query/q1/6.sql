select
    sum(l_extendedprice * l_discount) as revenue
from
    lineitem
where
        l_shipdate >= date '1994-08-11'
  and l_shipdate < date '1996-06-15'
  and l_discount between 0.04317728281021119 and  0.06987196207046509
  and l_quantity < 14.080395758152008;
