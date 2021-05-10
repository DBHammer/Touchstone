select
    sum(l_extendedprice * l_discount) as revenue
from
    lineitem
where
        l_shipdate >= date '1992-02-13'
  and l_shipdate < date '1993-12-18'
  and l_discount between 0.059052985906600956 and  0.08574767112731935
  and l_quantity < 14.080395758152008;
