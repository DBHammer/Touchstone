select
        count(*)
from
        part,
        supplier,
        partsupp,
        nation,
        region
where
        p_partkey = ps_partkey
        and s_suppkey = ps_suppkey
        and p_size = 27
        and p_type like '%eVYn8GlflADRMfLS222B%'
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'S'
        and part.P_PARTKEY in(
select
        partsupp.PS_PARTKEY
from
        supplier,
        partsupp,
        nation,
        region
where
        s_suppkey = ps_suppkey
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'wCqwKEhr')

