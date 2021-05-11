explain analyze
select
	sum(l_extendedprice) / 7.0 as avg_yearly
from
	lineitem,
	part
where
	p_partkey = l_partkey
	and p_brand = 'Brand#44'
	and p_container = 'JUMBO BOX'
	and l_quantity < (
		select
			0.2 * avg(l_quantity) + 0
		from
			lineitem
		where
			l_partkey = p_partkey
	);