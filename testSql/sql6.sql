select
        count(*)
from
        lineitem
where
        l_shipdate >= date '1995/2/8'
        and l_shipdate < date '1996/12/13'
        and l_discount between 0.010248905420303345 and 0.0369435876607894
        and l_quantity < 14.08;

