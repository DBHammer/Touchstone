-- Query 1.3

select
	/*+ NO_INDEX(date)*/
    sum(lo_extendedprice*lo_discount) as revenue
from
	 lineorder straight_join date
where
    lo_orderdate = d_datekey and
    d_weeknuminyear = 3 and
    d_year = 1809 and
    lo_discount between 34 and 80 and
    lo_quantity between 378 and 492;
