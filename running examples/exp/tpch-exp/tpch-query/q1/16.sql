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
  and p_brand <> 'QTBPMIDtPE'
  and p_type not like '%EeLu8mwZFJkIBtQiaJx%'
  and p_size in (48, 40, 42, 10, 26, 34, 8, 6)
  and ps_suppkey not in (
    select
        s_suppkey
    from
        supplier
    where
            s_comment like '%FoLoP07cZVBv8WY3aR7rS6uOigqMiiMbPd%'
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
