-- Query 1.3

select
	/*+ NO_INDEX(date)*/
    sum(lo_extendedprice*lo_discount) as revenue
from
	 lineorder straight_join date
where
    lo_orderdate = d_datekey and
    d_weeknuminyear = 30 and
    d_year = 1930 and
    lo_discount between 47 and 93 and
    lo_quantity between 363 and 477;
