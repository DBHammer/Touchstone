-- Query 1.1

select
    /*+ NO_INDEX(date)*/
sum(lo_extendedprice*lo_discount) as revenue
from
	 lineorder straight_join date
where
    lo_orderdate = d_datekey and
    d_year = 1948 and
    lo_discount between 36 and 109 and
    lo_quantity < 181;
