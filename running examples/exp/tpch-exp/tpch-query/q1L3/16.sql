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
  and p_brand <> 'sKI1VXM'
  and p_type not like '%ElzNRxGuokXWCkaD7aodfX%'
  and p_size in (49, 1, 37, 39, 16, 11, 7, 29)
  and ps_suppkey not in (
    select
        s_suppkey
    from
        supplier
    where
            s_comment like '%Z4yrUDDjMAAM7I2DB3Ct8Kr1BcVhiYVJbFmjBD45NfwEUV8aGHLPRKwvMdTZ5XtjMn92Rje2%'
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
