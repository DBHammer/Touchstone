	select
		l_suppkey,
		sum(l_extendedprice * (1 - l_discount))
	from
		lineitem
	where
		l_shipdate >= date '1993-12-01'
		and l_shipdate < date '1993-12-01' + interval '3' month
	group by
		l_suppkey
    limit 100;