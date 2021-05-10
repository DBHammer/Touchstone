-- Query 1.3

select
	/*+ NO_INDEX(date)*/
    sum(lo_extendedprice*lo_discount) as revenue
from
	 lineorder straight_join date
where
    lo_orderdate = d_datekey and
    d_weeknuminyear = 7 and
    d_year = 1908 and
    lo_discount between 113 and 159 and
    lo_quantity between 172 and 286;
