-- Query 1.1

select
    /*+ NO_INDEX(date)*/
sum(lo_extendedprice*lo_discount) as revenue
from
	 lineorder straight_join date
where
    lo_orderdate = d_datekey and
    d_year = 1805 and
    lo_discount between 8 and 80 and
    lo_quantity < 181;
