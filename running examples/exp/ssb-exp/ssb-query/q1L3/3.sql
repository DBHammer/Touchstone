-- Query 1.3

select
	/*+ NO_INDEX(date)*/
    sum(lo_extendedprice*lo_discount) as revenue
from
	 lineorder straight_join date
where
    lo_orderdate = d_datekey and
    d_weeknuminyear = 5 and
    d_year = 1875 and
    lo_discount between 131 and 177 and
    lo_quantity between 20 and 134;
