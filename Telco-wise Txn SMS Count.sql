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
- Email thread: Fwd: Telco Wise TallyKhata SMS Report from Inception
- Notes (if any): Hello Bhaiya, this is the much I could progress till now for SMS report. 
*/

-- mining mobile_no from txn SMS (~ 12 mins)
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select id, request_time, translate(substring(message_body, length(message_body)-10, 12), '০১২৩৪৫৬৭৮৯', '0123456789') mobile_no
from public.t_scsms_message_archive_v2
where
	1=1
	and channel='TALLYKHATA_TXN'
	and bank_name='SURECASH'
	and lower(message_body) not like '%verification code%'
	and telco_identifier_id in(66, 64, 61, 62, 49, 67) 
	and upper(message_status) in('SUCCESS', '0'); 

-- txn SMS stats (~ 5 mins)
select to_char(request_time, 'YYYY-MM') year_month, left(mobile_no, 3) telco, count(id) txn_sms_sent   
from data_vajapora.help_a
group by 1, 2 
order by 1, 2; 

-- reg stats
select to_char(created_at, 'YYYY-MM') year_month, left(mobile_number, 3) telco, count(id) reg_merchants 
from public.register_usermobile 
group by 1, 2
order by 1, 2; 
