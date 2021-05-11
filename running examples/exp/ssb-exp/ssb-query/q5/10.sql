-- Query 3.4

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
    (c_city='P4evqf1aU2' or c_city='KxZfnYHT5t') and
    (s_city='cx71Wz7jcG' or s_city='5q5bS7fBNg') and
    d_yearmonth = 'Br7nR1O'
group by c_city, s_city, d_year
order by d_year asc, revenue desc;
