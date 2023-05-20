drop table if exists FARRUHRUSYANDEXRU__DWH.l_user_group_activity;
create table FARRUHRUSYANDEXRU__DWH.l_user_group_activity (
	hk_l_user_group_activity int primary key,
	hk_user_id  int not null CONSTRAINT fk_l_user_group_activity_user REFERENCES FARRUHRUSYANDEXRU__DWH.h_users (hk_user_id),
	hk_group_id int not null CONSTRAINT fk_l_user_group_activity_group REFERENCES FARRUHRUSYANDEXRU__DWH.h_groups (hk_group_id),
	load_dt datetime,
	load_src varchar(20)
);

create table FARRUHRUSYANDEXRU__DWH.s_auth_history (
	hk_l_user_group_activity bigint not null 
		CONSTRAINT fk_s_admins_l_admins 
		REFERENCES FARRUHRUSYANDEXRU__DWH.l_user_group_activity (hk_l_user_group_activity),
	user_id_from int,
	event varchar(6),
	event_dt datetime,
	load_dt datetime,
	load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);