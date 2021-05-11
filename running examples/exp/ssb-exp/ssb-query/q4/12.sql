-- Query 4.2

select
   /*+  NO_INDEX(lineorder)
        NO_INDEX(date)
    	   NO_INDEX(part)
	   NO_INDEX(customer) */
	d_year, s_nation, p_category, sum(lo_revenue - lo_supplycost) as profit
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
    c_region = 'XYvMaFo' and
    s_region = 'UnQVAv9QWac' and
    (d_year = 1815 or d_year = 1883) and
    (p_mfgr = 'q8R26q' or p_mfgr = 'ANZoQy')
group by d_year, s_nation, p_category
order by d_year, s_nation, p_category;
