select count(*)                             as remark
from  loan t1
inner join  loanhandelinfo t2
        on t1.loan_no=t2.loan_no
       and t2.sign_time>='#2,0,1#'
left join
    (
    select  business_id
           ,max(complete_time) as complete_time
    from  tb_status
    where bill_status='#3,0,0#'
    group by business_id
    ) t3
on t1.loan_no=t3.business_id
left join
    (
    select  tt1.loan_no
           ,tt1.participant_id,tt1.participant_name
           ,tt2.superior_code,tt2.superior_name
    from  participantrecord tt1
    inner join  relation_ehr_user_org_position tt2
            on tt1.participant_id=tt2.usercode
           and tt2.position_type='#6,0,0#'
           and tt2.position_state='#5,0,0#'
    where tt1.role='#4,0,0#'
    ) t4
on t1.loan_no=t4.loan_no
left join  loan_capital_info t5
       on t1.loan_no=t5.loan_no
left join
    (
    select  order_no
           ,max(case when audit_type_id='RANSOM_AUDIT_SUBMIT' and last_audit_result='赎楼完成' then last_audit_time end) as last_kk
           ,max(case when audit_type_id='PLEDGE_AUDIT'        and last_audit_result='解押成功' then last_audit_time end) as last_qjy
           ,max(case when audit_type_id='RELIEVE_AUDIT'       and last_audit_result='解押成功' then last_audit_time end) as last_jy
    from  t_fact_risk_audit_accum
    where last_audit_result in ('#7,0,0#','#7,1,0#')
    group by order_no
    ) t6
on t1.loan_no=t6.order_no
left join
    (
    select  loan_code
           ,sum(case when performance_type!='REFUND' then performance_amount end) as perf_ss
           ,sum(performance_amount) as perf_jy
    from  performance
    where source_channel='#9,0,0#'
      and position_type='#8,0,0#'
    group by loan_code
    ) t7
on t1.loan_no=t7.loan_code
left join
    (
    select  com_code
           ,max(ori_name)     as ori_name
           ,max(pro_use_type) as pro_use_type
    from  dw_pro_product_list
    group by com_code
    ) t8
on t1.combination_code=t8.com_code
left join  loan_redeem_house t9
       on t1.loan_no=t9.loan_no
left join  loan1 t10
       on t1.loan_no=t10.loan_no
left join  tb_total_bill t11
       on t1.loan_no=t11.business_id
left join
    (
    select  business_id
           ,max(case when fee_model=3 then total_contract_fee end) as contract_fee_3
           ,max(case when fee_model=4 then total_contract_fee end) as contract_fee_4
    from  tb_receivable
    where fee_model in ('#10,0,0#','#10,1,0#')
    group by business_id
    ) t13
on t1.loan_no=t13.business_id
left join
    (
    select  code        as code
           ,max(name)   as name
    from  channelpool
    group by code
    ) t14
on t1.channel=t14.code
/* 剔除这些金融单状态："未知"，"解约中"，"已中止（解约）"，"已中止（无效）"，"已终结（未达成）"，"已中止（放款失败）" */
where t1.loan_status not in ('#1,0,0#','#1,1,0#','#1,2,0#','#1,3,0#','#1,4,0#')
  and t1.loan_status is not null
  and t1.city_code='#0,0,0#'
order by t2.sign_time desc;

select count(*)
from dw_add_cfinfo_dalian
where loan_no not in (select t1.loan_no
                      from loan t1
                               inner join loanhandelinfo t2
                                          on t1.loan_no=t2.loan_no
                                              and t2.sign_time>='2020-01-03 04:30:42'
                      where t1.loan_status not in ('GG', 's', 'A5', 'uP', 'krsl')
                        and t1.loan_status is not null
                        and t1.city_code='8H')