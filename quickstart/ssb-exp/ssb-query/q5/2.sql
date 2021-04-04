-- Query 1.2

select
	 /*+ NO_INDEX(date)*/
    sum(lo_extendedprice*lo_discount) as revenue
from
	 lineorder straight_join date
where
    lo_orderdate = d_datekey and
    d_yearmonthnum = 199310 and
    lo_discount between 141 and 187 and
    lo_quantity between 83 and 199;
