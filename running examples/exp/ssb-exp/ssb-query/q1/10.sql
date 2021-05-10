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
    (c_city='Xd1u0t5y9e' or c_city='FLN4gIrGhw') and
    (s_city='UmoTHjY5jT' or s_city='M66xGriB6D') and
    d_yearmonth = 'bUVJbaJ'
group by c_city, s_city, d_year
order by d_year asc, revenue desc;
