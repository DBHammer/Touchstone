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
    c_region = 'XmO10Xnt' and
    s_region = 'vcXDnqHF' and
    (d_year = 1918 or d_year = 1928) and
    (p_mfgr = 'qbVpi4' or p_mfgr = 'R7iW4d')
group by d_year, s_nation, p_category
order by d_year, s_nation, p_category;
