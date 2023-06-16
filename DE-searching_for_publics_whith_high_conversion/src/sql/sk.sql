create table if not exists E8ECA156YANDEXBY__STAGING.group_log(
group_id int not null,
user_id int null,
user_id_from int null,
event varchar(200) null,
event_dt datetime null
)
order by group_id
SEGMENTED BY HASH(group_id) ALL NODES;

/* Создание и наполнение хабов */

create table if not exists E8ECA156YANDEXBY__DWH.h_users
(
    hk_user_id bigint primary key,
    user_id      int,
    registration_dt datetime,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

create table if not exists E8ECA156YANDEXBY__DWH.h_groups
(
    hk_group_id bigint primary key,
    group_id      int,
    registration_dt datetime,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO E8ECA156YANDEXBY__DWH.h_users(hk_user_id, user_id, registration_dt, load_dt, load_src)
	with cte as (
	select distinct(id), registration_dt  from E8ECA156YANDEXBY__STAGING.users u
	inner join E8ECA156YANDEXBY__STAGING.group_log gl on u.id = gl.user_id 
	order by id)
	select hash(id) as  hk_user_id, id, registration_dt , now() as load_dt, 's3' as load_src  from cte
where hash(id) not in (select hk_user_id from E8ECA156YANDEXBY__DWH.h_users); 

INSERT INTO E8ECA156YANDEXBY__DWH.h_groups(hk_group_id, group_id, registration_dt, load_dt, load_src)
	with cte as (
	select  distinct(id) as group_id, registration_dt, now() as load_dt, 's3' as load_src  from E8ECA156YANDEXBY__STAGING.groups g
	inner join E8ECA156YANDEXBY__STAGING.group_log gl on g.id = gl.group_id 
	order by id)
	select hash(group_id) as hk_group_id, * from cte
where hash(group_id) not in (select hk_group_id from E8ECA156YANDEXBY__DWH.h_groups); 

/* Создание линка и вставка */

create table if not exists E8ECA156YANDEXBY__DWH.l_user_group_activity
(
hk_l_user_group_activity bigint primary key,
hk_user_id      bigint not null CONSTRAINT hk_l_user_group_activity_h_users REFERENCES E8ECA156YANDEXBY__DWH.h_users (hk_user_id),
hk_group_id bigint not null CONSTRAINT l_user_group_activity_h_groups REFERENCES E8ECA156YANDEXBY__DWH.h_groups (hk_group_id),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO E8ECA156YANDEXBY__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id, hk_group_id,  load_dt, load_src)
select distinct(hash(hu.user_id, hg.group_id)),
	hu.hk_user_id, 
	hg.hk_group_id,
	now() as load_dt,
	's3' as load_src
from E8ECA156YANDEXBY__STAGING.group_log gl 
left join E8ECA156YANDEXBY__DWH.h_users hu on gl.user_id = hu.user_id 
left join E8ECA156YANDEXBY__DWH.h_groups hg on gl.group_id = hg.group_id;

/* Создание сателлита и вставка */

create table if not exists E8ECA156YANDEXBY__DWH.s_auth_history
(
hk_l_user_group_activity bigint not null CONSTRAINT hk_l_user_group_activity_luga REFERENCES E8ECA156YANDEXBY__DWH.l_user_group_activity (hk_l_user_group_activity),
user_id_from int,
event varchar(20),
event_dt datetime,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

insert into E8ECA156YANDEXBY__DWH.s_auth_history(hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_src)
select hk_l_user_group_activity, 
	gl.user_id_from , 
	event, 
	event_dt, 
	now() as load_dt, 
	's3' as load_src
from E8ECA156YANDEXBY__STAGING.group_log as gl
left join E8ECA156YANDEXBY__DWH.h_groups as hg on gl.group_id = hg.group_id
left join E8ECA156YANDEXBY__DWH.h_users as hu on gl.user_id = hu.user_id
left join E8ECA156YANDEXBY__DWH.l_user_group_activity as luga on hg.hk_group_id = luga.hk_group_id


/* Ответ Бизнесу */

with cte as(
select group_id, 
	count(distinct(user_id)) as cnt_added_users 
	from E8ECA156YANDEXBY__STAGING.group_log
where event = 'add'
group by group_id
order by cnt_added_users desc),
cnt_added_users as (
select hk_group_id, 
	cnt_added_users, 
	hg.group_id, 
	hg.registration_dt from cte
join E8ECA156YANDEXBY__DWH.h_groups hg on cte.group_id = hg.group_id
order by hg.registration_dt),
cte_1 as (
select message_group, 
	count(distinct(message_from)) as cnt_users_in_group_with_messages 
from E8ECA156YANDEXBY__STAGING.dialogs d 
where message_group is not null
group by message_group),
user_group_messages as (
select hk_group_id, 
	cnt_users_in_group_with_messages from cte_1 
join E8ECA156YANDEXBY__DWH.h_groups hg on cte_1.message_group = hg.group_id)
select cau.hk_group_id, 
	cnt_added_users, 
	cnt_users_in_group_with_messages, 
	round(cnt_users_in_group_with_messages/ cnt_added_users, 2) as group_conversion, 
	registration_dt from cnt_added_users cau
join user_group_messages ugm on ugm.hk_group_id = cau.hk_group_id
order by registration_dt
