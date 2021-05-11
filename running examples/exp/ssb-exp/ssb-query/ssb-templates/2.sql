-- Query 1.2

select
	 /*+ NO_INDEX(date)*/
    sum(lo_extendedprice*lo_discount) as revenue
from
	 lineorder straight_join date
where
    lo_orderdate = d_datekey and
    d_yearmonthnum = #3,0,0# and
    lo_discount between #4,0,0# and #4,1,0# and
    lo_quantity between #5,0,0# and #5,1,0#;
