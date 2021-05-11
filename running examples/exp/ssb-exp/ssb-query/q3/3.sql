-- Query 1.3

select
	/*+ NO_INDEX(date)*/
    sum(lo_extendedprice*lo_discount) as revenue
from
	 lineorder straight_join date
where
    lo_orderdate = d_datekey and
    d_weeknuminyear = 25 and
    d_year = 1988 and
    lo_discount between 105 and 151 and
    lo_quantity between 201 and 315;
