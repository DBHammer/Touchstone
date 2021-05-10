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
  and p_brand <> 'XXNkLMN'
  and p_type not like '%sWYPIMZoC1C481CDMwR75%'
  and p_size in (39, 3, 48, 37, 43, 18, 14, 4)
  and ps_suppkey not in (
    select
        s_suppkey
    from
        supplier
    where
            s_comment like '%6NbXvxA02SZMQQYKNxNLuhA3GEddhifZi8miL5FQL2P7prIGyYEgzuTTTuwOa7W7BOARW1d8hT%'
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
