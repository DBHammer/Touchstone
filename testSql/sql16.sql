select
	count(*)
from
	partsupp,
	part
where
	p_partkey = ps_partkey
	and p_brand <> 'eiuLm7y0WO'
	and p_type not like '%pJ2kSAZ37MsGlz4bhyi%'
	and p_size in (30, 36, 39, 43, 17, 4, 31, 42)
	and ps_suppkey not in (
		select
			s_suppkey
		from
			supplier
		where
			s_comment like '%UFluD7HiSCVeNhcbpoCiIipo6Rp%'
	)