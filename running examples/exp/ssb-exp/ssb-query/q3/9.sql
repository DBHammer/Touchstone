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
    (c_city='V8tsDpkeGB' or c_city='ElgzWzMOmm') and
    (s_city='OfENFgDQ5D' or s_city='8YgKYLFZNZ') and
    d_year >= 1833.0 and
    d_year <= 1988.0
group by c_city, s_city, d_year
order by d_year asc, revenue desc;
