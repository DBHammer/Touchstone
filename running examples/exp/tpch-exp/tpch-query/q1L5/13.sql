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
                    and o_comment not like '%AnN4OoWyy7clKllWfFvRcpFoh51kmg3TBceN4f4QVYF5S1md%'
        group by
            c_custkey
    ) as c_orders (c_custkey, c_count)
group by
    c_count
order by
    custdist desc,
    c_count desc;
