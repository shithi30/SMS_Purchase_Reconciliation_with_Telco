/*
- Viz: 
- Data: 
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
	SMS: 4828350
	PUs: 409128 
	avg. SMS per PU: 11.8015
*/

drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select id, mobile_no
from public.t_scsms_message_archive_v2 as s
where
	upper(s.channel) in('TALLYKHATA_OTP','TALLYKHATA_TXN') and upper(trim(s.bank_name)) = 'SURECASH'
	and s.telco_identifier_id in(66, 64,61,62,49) and upper(s.message_status) in ('SUCCESS','0')
	and s.request_time::date >= '2021-01-01'::date and s.request_time < '2021-12-31'::date; 

drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as 
select distinct mobile_no 
from tallykhata.tk_power_users_10 
where report_date>='2021-01-01'::date and report_date<'2021-12-31'::date; 
	
select count(id) sms, count(distinct mobile_no) pu, count(id)*1.00/count(distinct mobile_no) avg_sms_pu
from 
	data_vajapora.help_a tbl1 
	inner join 
	data_vajapora.help_b tbl2 using(mobile_no); 

