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
  and p_brand <> '1FyGaKjdkd'
  and p_type not like '%gnuDaTrhbCZ3IN0T%'
  and p_size in (36, 4, 35, 14, 30, 42, 39, 5)
  and ps_suppkey not in (
    select
        s_suppkey
    from
        supplier
    where
            s_comment like '%jPIl3xc9QJIlxEUnjtFlVp8EPUiYSKOUENbEBqsaYo2sCEDY6L9iDI3xVDEEFpWVr8OOThSxepj%'
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
