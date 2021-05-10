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
    c_region = 'NB7a4S6J' and
    s_nation = 'RnMC8NJ101i' and
    (d_year = 1823 or d_year = 1855) and
    p_category = '0Mddt2E'
group by d_year, s_city, p_brand1
order by d_year, s_city, p_brand1;
