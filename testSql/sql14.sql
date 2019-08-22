select
    count(*)
from
        lineitem,
        part
where
        l_partkey = p_partkey
        and l_shipdate >= date '1998/3/28'
        and l_shipdate < date '1998/4/29'