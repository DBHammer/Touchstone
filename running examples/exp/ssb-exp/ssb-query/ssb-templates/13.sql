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
    c_region = '#39,0,0#' and
    s_nation = '#18,0,0#' and
    (d_year = #37,0,0# or d_year = #38,0,0#) and
    p_category = '#40,0,0#'
group by d_year, s_city, p_brand1
order by d_year, s_city, p_brand1;
