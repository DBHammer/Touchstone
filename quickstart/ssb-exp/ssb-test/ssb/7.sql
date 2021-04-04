-- Query 3.1

select
	       /*+ NO_INDEX(lineorder)
            NO_INDEX(date)
	 	   NO_INDEX(customer) */
	c_nation, s_nation, d_year, sum(lo_revenue) as revenue
from
	supplier straight_join lineorder
         straight_join date
         straight_join customer
where
    lo_custkey = c_custkey and
    lo_suppkey = s_suppkey and
    lo_orderdate = d_datekey and
    c_region = 'ASIA' and
    s_region = 'ASIA' and
    d_year >= 1992 and
    d_year <= 1997
group by c_nation, s_nation, d_year
order by d_year asc, revenue desc;
