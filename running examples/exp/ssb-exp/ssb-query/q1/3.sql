-- Query 1.3

select
	/*+ NO_INDEX(date)*/
    sum(lo_extendedprice*lo_discount) as revenue
from
	 lineorder straight_join date
where
    lo_orderdate = d_datekey and
    d_weeknuminyear = 7 and
    d_year = 1859 and
    lo_discount between 60 and 106 and
    lo_quantity between 6 and 120;
