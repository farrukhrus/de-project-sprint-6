drop table if exists FARRUHRUSYANDEXRU__STAGING.group_log;
create table FARRUHRUSYANDEXRU__STAGING.group_log(
	group_id int not null,
	user_id int not null,
	user_id_from int,
	event varchar(6),
	datetime timestamp
) order by group_id, user_id
SEGMENTED BY HASH(group_id) ALL NODES
PARTITION BY datetime::date
GROUP BY calendar_hierarchy_day(datetime::date, 3, 2);