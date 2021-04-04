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
  and p_brand <> 'nUGLyCNNS'
  and p_type not like '%wvfgbonWSEmgr32hhMBNjA%'
  and p_size in (1, 47, 42, 34, 36, 45, 44, 33)
  and ps_suppkey not in (
    select
        s_suppkey
    from
        supplier
    where
            s_comment like '%DU7Naykdmms0QuE7UuqYfKanHRY68VYh1DFOHfVss7aoJzF9C9McrBQgG0l9Vo0aWRKBFn4kzMP4RC3kF3%'
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
