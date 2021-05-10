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
    (c_city='JPCim3stnV' or c_city='lHodqbBTcJ') and
    (s_city='R4oriDDzKY' or s_city='UHgx6rJ7nR') and
    d_year >= 1815.0 and
    d_year <= 1968.0
group by c_city, s_city, d_year
order by d_year asc, revenue desc;
