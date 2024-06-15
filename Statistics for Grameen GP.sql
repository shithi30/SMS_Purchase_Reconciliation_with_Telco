/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1723724325
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation: 
- Email thread: Internet and SMS Usages Stats of TallyKhata
- Notes (if any): 
*/

-- Number of registered users: 1334030
select count(distinct mobile_number) reg_users
from public.register_usermobile
where left(mobile_number, 3) in('017', '013');

-- Number of MAUs: 278753
select ceil(avg(maus)) avg_maus
from
	(select to_char(event_date, 'YYYY-MM') year_month, count(distinct mobile_no) maus
	from tallykhata.tallykhata_user_date_sequence_final
	where 
		to_char(event_date, 'YYYY-MM') in('2021-04', '2021-05', '2021-06')
		and left(mobile_no, 3) in('017', '013')
	group by 1
	) tbl1;

-- Avg. TRT/day (all users): 360094, Avg. TRT/day (per user): 6.5951
select ceil(avg(daily_trt)) avg_trt_daily, avg(daily_avg_trt) avg_trt_daily_per_user
from 
	(select created_datetime, count(auto_id) daily_trt, count(distinct mobile_no) daily_merchants, count(auto_id)*1.00/count(distinct mobile_no) daily_avg_trt
	from tallykhata.tallykhata_fact_info_final 
	where 
		created_datetime>='2021-07-01' and created_datetime<='2021-07-20'
		and left(mobile_no, 3) in('017', '013')
	group by 1
	) tbl1; 

-- Avg. TRT/month (all users): 10175816, Avg. TRT/month (per user): 59.7633
select ceil(avg(monthly_trt)) avg_trt_monthly, avg(monthly_avg_trt) avg_trt_monthly_per_user
from 
	(select to_char(created_datetime, 'YYYY-MM') year_month, count(auto_id) monthly_trt, count(distinct mobile_no) monthly_merchants, count(auto_id)*1.00/count(distinct mobile_no) monthly_avg_trt
	from tallykhata.tallykhata_fact_info_final 
	where 
		to_char(created_datetime, 'YYYY-MM') in('2021-04', '2021-05', '2021-06')
		and left(mobile_no, 3) in('017', '013')
	group by 1
	) tbl1; 

-- Avg. TNX SMS/day (all users): 113777, Avg. TNX SMS/day (per user): 4.6538
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select id, date(request_time) request_date, translate(substring(message_body, length(message_body)-10, 12), '০১২৩৪৫৬৭৮৯', '0123456789') merchant_mobile
from public.t_scsms_message_archive_v2
where 
	date(request_time)>='2021-07-01' and date(request_time)<='2021-07-20'
	and message_status in('SUCCESS')
	and left(message_body, 9)!='মোট বাকি '
	and left(translate(substring(message_body, length(message_body)-10, 12), '০১২৩৪৫৬৭৮৯', '0123456789'), 3) in('017', '013'); 

select ceil(avg(daily_txn_sms)) avg_txn_sms_daily, avg(daily_avg_txn_sms) avg_txn_sms_daily_per_user
from 			
	(select request_date, count(id) daily_txn_sms, count(distinct merchant_mobile) daily_merchants, count(id)*1.00/count(distinct merchant_mobile) daily_avg_txn_sms	
	from data_vajapora.help_a
	group by 1
	) tbl1;

-- Avg. TNX SMS/month (all users): 2523472, Avg. TNX SMS/month (per user): 29.4168
drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as
select id, date(request_time) request_date, translate(substring(message_body, length(message_body)-10, 12), '০১২৩৪৫৬৭৮৯', '0123456789') merchant_mobile
from public.t_scsms_message_archive_v2
where 
	date(request_time)>='2021-04-01' and date(request_time)<='2021-06-30'
	and message_status in('SUCCESS')
	and left(message_body, 9)!='মোট বাকি '
	and left(translate(substring(message_body, length(message_body)-10, 12), '০১২৩৪৫৬৭৮৯', '0123456789'), 3) in('017', '013');

select ceil(avg(monthly_txn_sms)) avg_txn_sms_monthly, avg(monthly_avg_txn_sms) avg_txn_sms_monthly_per_user
from 
	(select to_char(request_date, 'YYYY-MM') year_month, count(id) monthly_txn_sms, count(distinct merchant_mobile) monthly_merchants, count(id)*1.00/count(distinct merchant_mobile) monthly_avg_txn_sms
	from data_vajapora.help_b
	where 
		to_char(request_date, 'YYYY-MM') in('2021-04', '2021-05', '2021-06')
		and left(merchant_mobile, 3) in('017', '013')
	group by 1
	) tbl1; 

-- Avg. tagada SMS/day (all users): 4916, Avg. tagada SMS/day (per user): 2.2185
select ceil(avg(daily_tagada_sms)) avg_tagada_sms_daily, avg(daily_avg_tagada_sms) avg_tagada_sms_daily_per_user
from 
	(select date, count(id) daily_tagada_sms, count(distinct merchant_mobile) daily_merchants, count(id)*1.00/count(distinct merchant_mobile) daily_avg_tagada_sms
	from public.notification_tagadasms
	where 
		date>='2021-07-01' and date<='2021-07-20'
		and left(merchant_mobile, 3) in('017', '013')
	group by 1
	) tbl1; 

-- Avg. tagada SMS/month (all users): 157024, Avg. tagada SMS/month (per user): 4.6849
select ceil(avg(monthly_tagada_sms)) avg_tagada_sms_monthly, avg(monthly_avg_tagada_sms) avg_tagada_sms_monthly_per_user
from 
	(select to_char(date, 'YYYY-MM') year_month, count(id) monthly_tagada_sms, count(distinct merchant_mobile) monthly_merchants, count(id)*1.00/count(distinct merchant_mobile) monthly_avg_tagada_sms
	from public.notification_tagadasms
	where 
		to_char(date, 'YYYY-MM') in('2021-04', '2021-05', '2021-06')
		and left(merchant_mobile, 3) in('017', '013')
	group by 1
	) tbl1; 

-- Avg. internet consumption/day (all users): 9032455.7220, Avg. internet consumption/day (per user): 51.5280
select sum(daily_kbs) avg_kb_daily, avg(daily_kbs) avg_kb_daily_per_user
from 
	(select user_id, sum(kbs) daily_kbs
	from test.tk_kb_size_analysis_v1
	where 
		user_id!=''
		and left(user_id, 3) in('017', '013')
	group by 1
	) tbl1;
	
-- Avg. internet consumption/month (all users): 234843848.7723, Avg. internet consumption/month (per user): 1339.7294
select sum(daily_kbs)*26 avg_kb_monthly, avg(daily_kbs)*26 avg_kb_monthly_per_user
from 
	(select user_id, sum(kbs) daily_kbs
	from test.tk_kb_size_analysis_v1
	where 
		user_id!=''
		and left(user_id, 3) in('017', '013')
	group by 1
	) tbl1;

-- Avg. duration of users with TallyKhata: 7.4708 months
select avg(days_with_tk)/30.00 months_with_tk
from 
	(select mobile_number, current_date-date(created_at)+1 days_with_tk
	from public.register_usermobile
	where left(mobile_number, 3) in('017', '013')
	) tbl1; 

/*
Number of registered users: 1334030
Number of MAUs: 278753
Avg. TRT/day (all users): 360094
Avg. TRT/day (per user): 6.5951
Avg. TRT/month (all users): 10175816
Avg. TRT/month (per user): 59.7633
Avg. TNX SMS/day (all users): 113777
Avg. TNX SMS/day (per user): 4.6538
Avg. TNX SMS/month (all users): 2523472
Avg. TNX SMS/month (per user): 29.4168
Avg. tagada SMS/day (all users): 4916
Avg. tagada SMS/day (per user): 2.2185
Avg. tagada SMS/month (all users): 157024
Avg. tagada SMS/month (per user): 4.6849
Avg. internet consumption/day (all users): 9032455.7220
Avg. internet consumption/day (per user): 51.5280
Avg. internet consumption/month (all users): 198714025.8842
Avg. internet consumption/month (per user): 1133.6171
Avg. duration of users with TallyKhata: 7.4708 months
*/
