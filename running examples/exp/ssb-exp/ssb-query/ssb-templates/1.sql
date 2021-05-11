-- Query 1.1

select
    /*+ NO_INDEX(date)*/
sum(lo_extendedprice*lo_discount) as revenue
from
	 lineorder straight_join date
where
    lo_orderdate = d_datekey and
    d_year = #0,0,0# and
    lo_discount between #1,0,0# and #1,1,0# and
    lo_quantity < #2,0,0#;
