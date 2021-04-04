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
    (c_city='aiQOqHTfgE' or c_city='iVDSZM3tlr') and
    (s_city='qPOse7nsx4' or s_city='uOU0iYF5PX') and
    d_yearmonth = '39Eg7PW'
group by c_city, s_city, d_year
order by d_year asc, revenue desc;
