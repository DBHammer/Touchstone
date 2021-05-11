-- Query 4.3

select
	 /*+ NO_INDEX(date)*/
    d_year, s_city, p_brand1, sum(lo_revenue - lo_supplycost) as profit
from
	supplier straight_join lineorder 
	straight_join date 
	straight_join part 
	straight_join customer
where
    lo_custkey = c_custkey and
    lo_suppkey = s_suppkey and
    lo_partkey = p_partkey and
    lo_orderdate = d_datekey and
    c_region = 'uKSdx0qny' and
    s_nation = '8F4c' and
    (d_year = 1879 or d_year = 1924) and
    p_category = 'gpQ3O71'
group by d_year, s_city, p_brand1
order by d_year, s_city, p_brand1;
