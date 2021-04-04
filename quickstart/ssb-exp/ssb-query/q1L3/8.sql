-- Query 3.2

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
    c_nation = 'Cm' and
    s_nation = '2kUo5EK4eDYy' and
    d_year >= 1805.0 and d_year <= 1980.0
group by c_city, s_city, d_year
order by d_year asc, revenue desc;
