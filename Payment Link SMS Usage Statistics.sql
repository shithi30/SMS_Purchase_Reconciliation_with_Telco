/*
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/1g2Hm6qmYJjB870MluhkS1y2-kDYfIGNLTOiCfb-cPdY/edit#gid=1476178625
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
	TK has added payment link in the transaction SMS on 16 Oct. It is for 5.0.x users. Each SMS contain the link.
	1. How many txn made, how many SMS sent, how many click happened on the link
	2. How many of them have wallet account and get payment 
*/

/*
-- audit external data tables
select count(*) from data_vajapora.version_info; 
select count(*) from data_vajapora.wallet_open; 
select count(*) from data_vajapora.cred_collection_payment; 
*/

-- version-05 merchants
-- bring from live data_vajapora.version_info
select mobile mobile_no, app_version_name, app_version_number, date(updated_at) update_date
from public.registered_users
where 
    device_status='active'
    and app_version_number>105; 
	
-- their txns
drop table if exists data_vajapora.help_c; 
create table data_vajapora.help_c as
select 
	created_datetime, 
	tbl1.mobile_no,
	count(auto_id) txns_recorded
from 
	(select mobile_no, auto_id, created_datetime 
	from tallykhata.tallykhata_fact_info_final
	) tbl1 
	inner join 
	data_vajapora.version_info tbl2 on(tbl1.mobile_no=tbl2.mobile_no and created_datetime>=update_date) 
group by 1, 2; 	

-- SMS usage after update
drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select 
	id, 
	translate(substring(message_body, length(message_body)-10, 12), '০১২৩৪৫৬৭৮৯', '0123456789') mobile_no, 
	case 
		-- when message_body like '%অনুগ্রহ করে%' then 'tagada'
		-- when message_body like '%পেমেন্ট লিংকঃ%' then 'txn link'
		when message_body like '%http%' then 'txn link'
		else 'txn'
	end sms_type, 
	date(request_time) sms_date 
from public.t_scsms_message_archive_v2 as s
where
	upper(s.channel) in('TALLYKHATA_TXN') 
	and upper(trim(s.bank_name)) = 'SURECASH'
	and lower(s.message_body) not like '%verification code%'
	and s.telco_identifier_id in(66, 64, 61, 62, 49, 67) 
	and upper(s.message_status) in ('SUCCESS', '0') 
	and date(request_time)>=(select min(update_date) from data_vajapora.version_info); 

/*
-- sample 'txn link' SMS 
select * 
from
	(select id
	from data_vajapora.help_b
	where 
		mobile_no in(
		'01755394303',
		'01726976327',
		'01926864678',
		'01645146925',
		'01722050693',
		'01972192030',
		'01770951820',
		'01722928658',
		'01304411211',
		'01311406262'
		)
		and sms_type='txn link'
	) tbl1 
	
	inner join 
	
	(select id, message_body
	from public.t_scsms_message_archive_v2 as s
	) tbl2 using(id)
order by mobile_no;
*/ 

-- created wallet
-- bring from TP DWH udap data_vajapora.wallet_open 
select distinct p.wallet_no mobile_no
from ods_tp.backend_db__profile p 
left join ods_tp.backend_db__document as d on p.user_id  = d.user_id 
left join ods_tp.backend_db__bank_account  as a on p.user_id  = a.user_id
left join ods_tp.backend_db__mfs_account as m on p.user_id  = m.user_id
where 1=1
and upper(d.doc_type) ='NID'
and p.created_at::date>='2022-09-21'
and p.bank_account_status = 'VERIFIED'; 

-- received payment
-- bring from TK DWH nobopay_dwh data_vajapora.cred_collection_payment
select distinct mobile_no, 'payment' act_type
from 
    backend_db.np_txn_log tbl1 
    inner join 
    (select user_id to_id, wallet_no mobile_no, bank_account_status
	from backend_db.profile
	) tbl2 using(to_id)
where 
    txn_type in ('CREDIT_COLLECTION')
    and status='COMPLETE'; 

-- desired stats (combined)
select 
	count(tbl1.mobile_no) merchants_recorded_txn, 
	sum(tbl1.txns_recorded) txns_recorded, 
	count(tbl2.mobile_no) merchants_used_link_sms, 
	sum(tbl2.link_sms_used) link_sms_used, 
	count(tbl3.mobile_no) merchants_created_wallet, 
	count(case when tbl3.mobile_no is not null then tbl4.mobile_no else null end) merchants_received_payment
from 
	(select mobile_no, sum(txns_recorded) txns_recorded
	from data_vajapora.help_c 
	group by 1
	) tbl1
	
	left join
	
	(select mobile_no, count(id) link_sms_used 
	from data_vajapora.help_b 
	where sms_type='txn link'
	group by 1
	) tbl2 using(mobile_no) 
	
	left join 
	
	data_vajapora.wallet_open tbl3 using(mobile_no) 
	
	left join 
	
	data_vajapora.cred_collection_payment tbl4 using(mobile_no); 

-- desired stats (combined, for wallet uses)
select 
	count(tbl1.mobile_no) merchants_recorded_txn, 
	sum(tbl1.txns_recorded) txns_recorded, 
	count(tbl2.mobile_no) merchants_used_link_sms, 
	sum(tbl2.link_sms_used) link_sms_used, 
	count(tbl3.mobile_no) merchants_created_wallet, 
	count(case when tbl3.mobile_no is not null then tbl4.mobile_no else null end) merchants_received_payment
from 
	(select mobile_no, sum(txns_recorded) txns_recorded
	from data_vajapora.help_c 
	group by 1
	) tbl1
	
	left join
	
	(select mobile_no, count(id) link_sms_used 
	from data_vajapora.help_b 
	where sms_type='txn link'
	group by 1
	) tbl2 using(mobile_no) 
	
	left join 
	
	data_vajapora.cred_collection_payment tbl4 using(mobile_no) 

	inner join 
	
	data_vajapora.wallet_open tbl3 using(mobile_no); 

-- desired stats (daily)
with 
	txn_stats as 
	(-- desired stats (txn)
	select 
		created_datetime, 
		count(mobile_no) merchants_recorded_txn, 
		sum(txns_recorded) txns_recorded
	from data_vajapora.help_c 
	group by 1
	), 
	
	sms_stats as 
	(-- desired stats (sms)
	select 
		sms_date created_datetime, 
		count(distinct mobile_no) link_sms_user, 
		sum(tbl2.link_sms_used) link_sms_used
	from 
		(select mobile_no, sum(txns_recorded) txns_recorded
		from data_vajapora.help_c 
		group by 1
		) tbl1
		
		left join
		
		(select mobile_no, sms_date, count(id) link_sms_used 
		from data_vajapora.help_b 
		where sms_type='txn link'
		group by 1, 2
		) tbl2 using(mobile_no) 
	group by 1
	)
	
select *
from 
	(-- dates
	select created_datetime from txn_stats where created_datetime is not null
	union 
	select created_datetime from sms_stats where created_datetime is not null
	) tbl1 
	
	left join 
	
	txn_stats tbl2 using(created_datetime)
	
	left join 

	sms_stats tbl3 using(created_datetime)
order by created_datetime; 

/* for Tahseen sheet */ 

-- version-05 merchants
select mobile mobile_no, app_version_name, app_version_number, date(updated_at) update_date
from public.registered_users
where 
    device_status='active'
    and app_version_number=116; 
	
-- stats
select 
	count(tbl1.mobile_no) merchants_503, 
	count(tbl2.mobile_no) merchants_recorded_cred_txn, 
	sum(cred_txns) total_recorded_cred_txn, 
	count(tbl4.mobile_no) merchants_sent_txn_sms, 
	sum(txn_sms_sent) total_txn_sms_sent,
	count(tbl3.mobile_no) merchants_sent_link_txn_sms, 
	sum(txn_link_sms_sent) total_link_txn_sms_sent
from 
	(select mobile_no
	from data_vajapora.version_info
	) tbl1 
	
	left join 
	
	(select mobile_no, sum(txns_recorded) cred_txns 
	from data_vajapora.help_c 
	group by 1
	) tbl2 using(mobile_no) 
	
	left join 
	
	(select mobile_no, count(id) txn_link_sms_sent
	from data_vajapora.help_b 
	where sms_type in('txn link')
	group by 1 
	) tbl3 using(mobile_no) 
	
	left join 
	
	(select mobile_no, count(id) txn_sms_sent
	from data_vajapora.help_b 
	where sms_type in('txn link', 'txn')
	group by 1 
	) tbl4 using(mobile_no); 
