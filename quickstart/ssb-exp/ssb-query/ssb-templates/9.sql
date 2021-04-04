-- Query 3.3

select
	   /*+ NO_INDEX(date)
    	  NO_INDEX(lineorder)
	    NO_INDEX(customer)*/
c_city, s_city, d_year, sum(lo_revenue) as revenue
from
	supplier straight_join lineorder
         straight_join date
         straight_join customer
where
    lo_custkey = c_custkey and
    lo_suppkey = s_suppkey and
    lo_orderdate = d_datekey and
    (c_city='#24,0,0#' or c_city='#25,0,0#') and
    (s_city='#21,0,0#' or s_city='#22,0,0#') and
    d_year >= #23,0,0# and
    d_year <= #23,1,0#
group by c_city, s_city, d_year
order by d_year asc, revenue desc;
