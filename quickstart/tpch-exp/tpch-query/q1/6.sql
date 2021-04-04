select
    sum(l_extendedprice * l_discount) as revenue
from
    lineitem
where
        l_shipdate >= date '1993-02-08'
  and l_shipdate < date '1994-12-14'
  and l_discount between 0.025603187084198 and  0.05229787826538086
  and l_quantity < 14.080395758152008;
