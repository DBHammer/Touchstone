-- Query 1.3

select
	/*+ NO_INDEX(date)*/
    sum(lo_extendedprice*lo_discount) as revenue
from
	 lineorder straight_join date
where
    lo_orderdate = d_datekey and
    d_weeknuminyear = 22 and
    d_year = 1900 and
    lo_discount between 104 and 150 and
    lo_quantity between 106 and 220;
