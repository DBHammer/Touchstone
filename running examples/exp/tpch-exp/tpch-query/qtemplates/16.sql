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
  and p_brand <> '#35,0,0#'
  and p_type not like '%#36,0,0#%'
  and p_size in (#37,0,0#, #37,1,0#, #37,2,0#, #37,3,0#, #37,4,0#, #37,5,0#, #37,6,0#, #37,7,0#)
  and ps_suppkey not in (
    select
        s_suppkey
    from
        supplier
    where
            s_comment like '%#38,0,0#%'
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
