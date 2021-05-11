select
    c_count,
    count(*) as custdist
from
    (
        select
            c_custkey,
            count(o_orderkey)
        from
            customer left outer join orders on
                        c_custkey = o_custkey
                    and o_comment not like '%5UZG1NBLxLz8OS2fr6t7G1kaoepDZz27uRQRVRMKII8f7B%'
        group by
            c_custkey
    ) as c_orders (c_custkey, c_count)
group by
    c_count
order by
    custdist desc,
    c_count desc;
