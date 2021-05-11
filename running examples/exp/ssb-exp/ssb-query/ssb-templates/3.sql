-- Query 1.3

select
	/*+ NO_INDEX(date)*/
    sum(lo_extendedprice*lo_discount) as revenue
from
	 lineorder straight_join date
where
    lo_orderdate = d_datekey and
    d_weeknuminyear = #7,0,0# and
    d_year = #6,0,0# and
    lo_discount between #8,0,0# and #8,1,0# and
    lo_quantity between #9,0,0# and #9,1,0#;
