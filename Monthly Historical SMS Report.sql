/*
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/1mqHGxP-drrgR70ICnF0PDvlc5lfZGk7qKWtNDrQNLqU/edit#gid=0 
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any):
	- Takes ~7 mins
	- Awlad Bhai's analysis: https://docs.google.com/spreadsheets/d/17dVb3hMsfCkn5i4LKJr88mE48kzu4bosVbP_265Ul30/edit#gid=2125268269
*/

-- daily statistics 
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select
	to_char(s.request_time, 'yyyy-mm-dd') as year_month_dd,
	count(case when s.telco_identifier_id in(61, 62) and lower(s.message_body) like '%verification code%' then s.id end) as GP_otp_sms_cnt,
	count(case when s.telco_identifier_id in(61, 62) and lower(s.message_body) not like '%verification code%' then s.id end) as GP_txn_sms_cnt,
	count(case when s.telco_identifier_id in(49) and lower(s.message_body) like '%verification code%' then s.id end) as Mobireach_otp_sms_cnt,
	count(case when s.telco_identifier_id in(49) and lower(s.message_body) not like '%verification code%' then s.id end) as Mobireach_txn_sms_cnt,
	count(case when s.telco_identifier_id in(64, 66) and lower(s.message_body) like '%verification code%' then s.id end) as Banglalink_otp_sms_cnt,
	count(case when s.telco_identifier_id in(64, 66) and lower(s.message_body) not like '%verification code%' then s.id end) as Banglalink_txn_sms_cnt
from
	public.t_scsms_message_archive_v2 as s
where
	upper(s.channel) in('TALLYKHATA_OTP','TALLYKHATA_TXN') and upper(trim(s.bank_name)) = 'SURECASH'
	and s.telco_identifier_id in(66, 64,61,62,49) and upper(s.message_status) in ('SUCCESS','0')
	and s.request_time::date >= '2020-01-01'::date and s.request_time < current_date
group by
	year_month_dd;

-- daily statistics, corrected for previous modality
drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select 
	s.year_month_dd,
	GP_otp_sms_cnt,
	Mobireach_otp_sms_cnt,
	Banglalink_otp_sms_cnt,
	case when year_month_dd::date <= '2020-12-02' then GP_txn_sms_cnt*2 else GP_txn_sms_cnt end as GP_txn_sms_cnt ,
	case when year_month_dd::date <= '2020-12-02' then Mobireach_txn_sms_cnt*2 else Mobireach_txn_sms_cnt end as Mobireach_txn_sms_cnt ,
	case when year_month_dd::date <= '2020-12-02' then Banglalink_txn_sms_cnt*2 else Banglalink_txn_sms_cnt end as Banglalink_txn_sms_cnt,
	(GP_otp_sms_cnt+Mobireach_otp_sms_cnt+Banglalink_otp_sms_cnt) as total_otp_sms,
	(case when year_month_dd::date <= '2020-12-02' then GP_txn_sms_cnt*2 else GP_txn_sms_cnt end
	+
	case when year_month_dd::date <= '2020-12-02' then Mobireach_txn_sms_cnt*2 else Mobireach_txn_sms_cnt end
	+
	case when year_month_dd::date <= '2020-12-02' then Banglalink_txn_sms_cnt*2 else Banglalink_txn_sms_cnt end
	) as total_txn_sms
from data_vajapora.help_a as s;

-- monthly statistics
select 
	left(year_month_dd, 7) year_month, 
	
	sum(gp_otp_sms_cnt) gp_otp_sms_cnt, 
	sum(mobireach_otp_sms_cnt) mobireach_otp_sms_cnt, 
	sum(banglalink_otp_sms_cnt) banglalink_otp_sms_cnt, 
	
	sum(gp_txn_sms_cnt) gp_txn_sms_cnt, 
	sum(mobireach_txn_sms_cnt) mobireach_txn_sms_cnt, 
	sum(banglalink_txn_sms_cnt) banglalink_txn_sms_cnt, 
	
	sum(total_otp_sms) total_otp_sms, 
	sum(total_txn_sms) total_txn_sms
from data_vajapora.help_b
group by 1
order by 1; 
