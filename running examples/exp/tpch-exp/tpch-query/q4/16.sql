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
  and p_brand <> 'FEeT8yew'
  and p_type not like '%ynBLsYr5Pum0Qo3%'
  and p_size in (12, 3, 49, 5, 13, 7, 31, 18)
  and ps_suppkey not in (
    select
        s_suppkey
    from
        supplier
    where
            s_comment like '%At45Vb6UZE3Wqk4PZczsM9HUFgQ0HlAdKJdBomucS7GKjWZcQFb3DItD7GSIuE7lBSapFOUbZt42JcQbTk%'
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
