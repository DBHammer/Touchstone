-- Query 1.2

select
	 /*+ NO_INDEX(date)*/
    sum(lo_extendedprice*lo_discount) as revenue
from
	 lineorder straight_join date
where
    lo_orderdate = d_datekey and
    d_yearmonthnum = 199659 and
    lo_discount between 89 and 136 and
    lo_quantity between 86 and 203;
