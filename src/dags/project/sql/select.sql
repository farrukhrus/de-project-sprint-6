-- 7.1
with user_group_messages as(
	select 
	    g.hk_group_id, 
	    count(distinct lum.hk_user_id) cnt_users_in_group_with_messages
	from FARRUHRUSYANDEXRU__DWH.h_groups g
	inner join FARRUHRUSYANDEXRU__DWH.l_groups_dialogs gd on g.hk_group_id = gd.hk_group_id 
	inner join FARRUHRUSYANDEXRU__DWH.l_user_message lum on gd.hk_message_id = lum.hk_message_id
	group by 1
)
select * from user_group_messages;

-- 7.2
with user_group_log as
    select 
        luga.hk_group_id,
        count(distinct luga.hk_user_id) as cnt_added_users
    from FARRUHRUSYANDEXRU__DWH.s_auth_history ah
    inner join FARRUHRUSYANDEXRU__DWH.l_user_group_activity luga
        on ah.hk_l_user_group_activity = luga.hk_l_user_group_activity
    inner join (select group_id from FARRUHRUSYANDEXRU__DWH.h_groups order by registration_dt asc limit 10) as hg 
        on hg.group_id = luga.hk_group_id 
    where ah.event = 'add'
    group by luga.hk_group_id
)
select * from user_group_log;

-- 7.3
with user_group_log as (
	select 
	    g.hk_group_id, 
	    count(distinct lum.hk_user_id) as cnt_users_in_group_with_messages
	from FARRUHRUSYANDEXRU__DWH.h_groups g
	inner join FARRUHRUSYANDEXRU__DWH.l_groups_dialogs gd on g.hk_group_id = gd.hk_group_id 
	inner join FARRUHRUSYANDEXRU__DWH.l_user_message lum on gd.hk_message_id = lum.hk_message_id
	group by 1
),
user_group_messages as (
	select 
        luga.hk_group_id,
        count(distinct luga.hk_user_id) as cnt_added_users
    from FARRUHRUSYANDEXRU__DWH.s_auth_history ah
    inner join FARRUHRUSYANDEXRU__DWH.l_user_group_activity luga
        on ah.hk_l_user_group_activity = luga.hk_l_user_group_activity
    inner join (select group_id from FARRUHRUSYANDEXRU__DWH.h_groups order by registration_dt asc limit 10) as hg 
        on hg.group_id = luga.hk_group_id 
    where ah.event = 'add'
    group by luga.hk_group_id
)
select 
	ugl.hk_group_id,
	cnt_added_users,
	cnt_users_in_group_with_messages,
	cnt_users_in_group_with_messages / cnt_added_users as group_conversion
from user_group_log as ugl
left join user_group_messages as ugm on ugl.hk_group_id = ugm.hk_group_id
order by cnt_users_in_group_with_messages / cnt_added_users desc 