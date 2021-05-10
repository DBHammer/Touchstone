select
    sum(l_extendedprice * l_discount) as revenue
from
    lineitem
where
        l_shipdate >= date '1992-11-20'
  and l_shipdate < date '1994-09-25'
  and l_discount between 0.01722577214241028 and  0.04392045736312866
  and l_quantity < 14.080395758152008;
