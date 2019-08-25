select
        count(*)
from
        lineitem
where
        l_shipdate >=  '1993-1-3'
        and l_shipdate < '1994-11-8'
        and l_discount between 0.05585016012191773 and 0.08254484534263612
        and l_quantity < 14.08;

select COUNT(*) from lineitem