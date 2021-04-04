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
                    and o_comment not like '%j3tphWNMhYL5I1RZdd0XDw2n1yQNF0KpczsP9fFXiE570aRQEMcweXOvBjk7Yik6aA%'
        group by
            c_custkey
    ) as c_orders (c_custkey, c_count)
group by
    c_count
order by
    custdist desc,
    c_count desc;
