select
	p_brand,
	p_type,
	p_size,
	count(distinct ps_suppkey) as supplier_cnt
from
		part straight_join 
    partsupp
where
	p_partkey = ps_partkey
	and p_brand <> 'Brand#12'
	and p_type not like 'MEDIUM BRUSHED%'
	and p_size in (27, 42, 18, 45, 46, 34, 14, 6)
	and ps_suppkey not in (
		select
			s_suppkey
		from
			supplier
		where
			s_comment like '%Customer%Complaints%'
	)
group by
	p_brand,
	p_type,
	p_size
order by
	supplier_cnt desc,
	p_brand,
	p_type,
	p_size;
