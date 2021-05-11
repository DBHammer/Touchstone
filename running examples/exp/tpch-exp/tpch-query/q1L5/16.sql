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
  and p_brand <> 'lVNS4X'
  and p_type not like '%DO6Acg2RJtoq66YB0cH2f%'
  and p_size in (14, 46, 21, 31, 44, 19, 6, 3)
  and ps_suppkey not in (
    select
        s_suppkey
    from
        supplier
    where
            s_comment like '%DpJ3FuZlXqwMHIX3KSvIkO20W8JSQmIKRfSl1QUjlU95uorwRVZVbiT0sPRA1yBwzoj1jEQPnPFqXTufi2aWy6OyoVVCr%'
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
