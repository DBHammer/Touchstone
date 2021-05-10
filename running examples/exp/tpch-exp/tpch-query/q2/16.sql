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
  and p_brand <> 'WPr299m'
  and p_type not like '%5KmUn4ydk4lv5m%'
  and p_size in (24, 29, 45, 30, 1, 38, 31, 14)
  and ps_suppkey not in (
    select
        s_suppkey
    from
        supplier
    where
            s_comment like '%AzQWAulJKsYZkXSbSyiSyc9XBqv1Du9RFCLgPRl5iY6gRBp13Nl3F5Y1GfgzC6xobX84uVwJMT7fjVlEa%'
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
