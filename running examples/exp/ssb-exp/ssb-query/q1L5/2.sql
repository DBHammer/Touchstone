-- Query 1.2

select
	 /*+ NO_INDEX(date)*/
    sum(lo_extendedprice*lo_discount) as revenue
from
	 lineorder straight_join date
where
    lo_orderdate = d_datekey and
    d_yearmonthnum = 199746 and
    lo_discount between 92 and 138 and
    lo_quantity between 318 and 434;
