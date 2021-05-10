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
    (c_city='xhInx7kRA0' or c_city='I78EJ9EAl5') and
    (s_city='ZgZwGYleFw' or s_city='RNpSINZjRT') and
    d_year >= 1823.0 and
    d_year <= 1966.0
group by c_city, s_city, d_year
order by d_year asc, revenue desc;
