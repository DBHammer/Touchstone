select
    sum(l_extendedprice * l_discount) as revenue
from
    lineitem
where
        l_shipdate >= date '1992-08-08'
  and l_shipdate < date '1994-06-13'
  and l_discount between 0.023363173007965088 and  0.050057864189147955
  and l_quantity < 14.080395758152008;
