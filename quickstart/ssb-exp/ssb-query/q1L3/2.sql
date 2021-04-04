-- Query 1.2

select
	 /*+ NO_INDEX(date)*/
    sum(lo_extendedprice*lo_discount) as revenue
from
	 lineorder straight_join date
where
    lo_orderdate = d_datekey and
    d_yearmonthnum = 199324 and
    lo_discount between 45 and 91 and
    lo_quantity between 355 and 471;
