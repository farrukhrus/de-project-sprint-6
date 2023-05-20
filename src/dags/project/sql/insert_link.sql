INSERT INTO FARRUHRUSYANDEXRU__DWH.l_user_group_activity
    (hk_l_user_group_activity, hk_user_id,hk_group_id,load_dt,load_src)
select distinct hash(gl.group_id,gl.user_id),
    hu.hk_user_id ,
    hg.hk_group_id ,
    datetime,
    's3' as load_src
from FARRUHRUSYANDEXRU__STAGING.group_log as gl
left join FARRUHRUSYANDEXRU__DWH.h_users hu on hash(gl.user_id) = hu.hk_user_id 
left join FARRUHRUSYANDEXRU__DWH.h_groups hg on hash(gl.group_id) = hg.hk_group_id;