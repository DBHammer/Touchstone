select
    sum(l_extendedprice * l_discount) as revenue
from
    lineitem
where
        l_shipdate >= date '1995-01-04'
  and l_shipdate < date '1996-11-08'
  and l_discount between 0.07175663113594055 and  0.09845131635665894
  and l_quantity < 14.080395758152008;
