-- Query 2.1

select
	 /*+ NO_INDEX(date)
    	   NO_INDEX(part)*/
	sum(lo_revenue), d_year, p_brand1
from
	supplier straight_join lineorder
         straight_join date
         straight_join part
where
    lo_orderdate = d_datekey and
    lo_partkey = p_partkey and
    lo_suppkey = s_suppkey and
    p_category = '#11,0,0#' and
    s_region = '#10,0,0#'
group by d_year, p_brand1
order by d_year, p_brand1;
