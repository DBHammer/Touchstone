select
    sum(l_extendedprice * l_discount) as revenue
from
    lineitem
where
        l_shipdate >= date '#12,0,1#'
  and l_shipdate < date '#12,1,1#'
  and l_discount between #13,0,0# and  #13,1,0#
  and l_quantity < #14,0,0#;