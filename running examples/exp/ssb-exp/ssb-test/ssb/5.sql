-- Query 2.2

select
        /*+ NO_INDEX(lineorder)
            NO_INDEX(date)
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
    p_brand1 between 'MFGR#2221' and 'MFGR#2228' and
    s_region = 'ASIA'
group by d_year, p_brand1
order by d_year, p_brand1;
