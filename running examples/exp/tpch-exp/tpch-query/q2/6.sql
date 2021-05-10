select
    sum(l_extendedprice * l_discount) as revenue
from
    lineitem
where
        l_shipdate >= date '1994-05-09'
  and l_shipdate < date '1996-03-13'
  and l_discount between 0.02885371446609497 and  0.05554839372634888
  and l_quantity < 14.080395758152008;
