CREATE OR REPLACE FUNCTION campaign_analytics.fn_monthly_sms_stats()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

/*
Authored by             : Shithi Maitra
Supervised by           : Md. Nazrul Islam
Purpose                 : Analysis of monthly SMS consumption
Auxiliary data table(s) : campaign_analytics.sms_help_a, campaign_analytics.sms_help_b, campaign_analytics.sms_help_c
Target data table(s)    : campaign_analytics.monthly_sms_stats_count, campaign_analytics.monthly_sms_stats_cost
data			: https://docs.google.com/spreadsheets/d/1mqHGxP-drrgR70ICnF0PDvlc5lfZGk7qKWtNDrQNLqU/edit#gid=1536464937
*/

declare 
	
begin 
	raise notice 'SMS stats generation started'; 
	
	-- daily statistics 
	drop table if exists campaign_analytics.sms_help_a; 
	create table campaign_analytics.sms_help_a as
	select
		to_char(s.request_time, 'yyyy-mm-dd') as year_month_dd,
		count(case when s.telco_identifier_id in(61, 62) and lower(s.message_body) like '%verification code%' then s.id end) as GP_otp_sms_cnt,
		count(case when s.telco_identifier_id in(61, 62) and lower(s.message_body) not like '%verification code%' then s.id end) as GP_txn_sms_cnt,
		count(case when s.telco_identifier_id in(49) and lower(s.message_body) like '%verification code%' then s.id end) as Mobireach_otp_sms_cnt,
		count(case when s.telco_identifier_id in(49) and lower(s.message_body) not like '%verification code%' then s.id end) as Mobireach_txn_sms_cnt,
		count(case when s.telco_identifier_id in(64, 66) and lower(s.message_body) like '%verification code%' then s.id end) as Banglalink_otp_sms_cnt,
		count(case when s.telco_identifier_id in(64, 66) and lower(s.message_body) not like '%verification code%' then s.id end) as Banglalink_txn_sms_cnt, 
		count(case when s.telco_identifier_id in(67) and lower(s.message_body) like '%verification code%' then s.id end) as adn_otp_sms_cnt,
		count(case when s.telco_identifier_id in(67) and lower(s.message_body) not like '%verification code%' then s.id end) as adn_txn_sms_cnt
	from
		public.t_scsms_message_archive_v2 as s
	where
		upper(s.channel) in('TALLYKHATA_OTP','TALLYKHATA_TXN') and upper(trim(s.bank_name)) = 'SURECASH'
		and s.telco_identifier_id in(66, 64,61,62,49, 67) and upper(s.message_status) in ('SUCCESS','0')
		and s.request_time::date >= '2020-01-01'::date and s.request_time < current_date
	group by
		year_month_dd;
	raise notice 'Daily stats generated'; 
	
	-- daily statistics, corrected for previous modality
	drop table if exists campaign_analytics.sms_help_b; 
	create table campaign_analytics.sms_help_b as
	select 
		s.year_month_dd,
		
		GP_otp_sms_cnt,
		Mobireach_otp_sms_cnt,
		Banglalink_otp_sms_cnt,
		adn_otp_sms_cnt, 
		
		case when year_month_dd::date <= '2020-12-02' then GP_txn_sms_cnt*2 else GP_txn_sms_cnt end as GP_txn_sms_cnt ,
		case when year_month_dd::date <= '2020-12-02' then Mobireach_txn_sms_cnt*2 else Mobireach_txn_sms_cnt end as Mobireach_txn_sms_cnt ,
		case when year_month_dd::date <= '2020-12-02' then Banglalink_txn_sms_cnt*2 else Banglalink_txn_sms_cnt end as Banglalink_txn_sms_cnt,
		case when year_month_dd::date <= '2020-12-02' then adn_txn_sms_cnt*2 else adn_txn_sms_cnt end as adn_txn_sms_cnt,
		
		(GP_otp_sms_cnt+Mobireach_otp_sms_cnt+Banglalink_otp_sms_cnt+adn_otp_sms_cnt) as total_otp_sms,
		(case when year_month_dd::date <= '2020-12-02' then GP_txn_sms_cnt*2 else GP_txn_sms_cnt end
		+
		case when year_month_dd::date <= '2020-12-02' then Mobireach_txn_sms_cnt*2 else Mobireach_txn_sms_cnt end
		+
		case when year_month_dd::date <= '2020-12-02' then Banglalink_txn_sms_cnt*2 else Banglalink_txn_sms_cnt end
		+
		case when year_month_dd::date <= '2020-12-02' then adn_txn_sms_cnt*2 else adn_txn_sms_cnt end
		) as total_txn_sms
	from campaign_analytics.sms_help_a as s;
	raise notice 'Daily stats corrected for previous modality'; 
	
	-- mining mobile_no from Tagada SMS (~ 15 mins)
	drop table if exists campaign_analytics.sms_help_c; 
	create table campaign_analytics.sms_help_c as
	select id, telco_identifier_id, request_time, translate(substring(message_body, length(message_body)-10, 12), '০১২৩৪৫৬৭৮৯', '0123456789') mobile_no
	from public.t_scsms_message_archive_v2
	where
		1=1
		and channel='TALLYKHATA_TXN'
		and bank_name='SURECASH'
		and message_body like '%অনুগ্রহ করে%'
		and telco_identifier_id in(66, 64, 61, 62, 49, 67) 
		and upper(message_status) in('SUCCESS', '0'); 
	raise notice 'Tagadas identified'; 
	
	-- monthly statistics (count)
	drop table if exists campaign_analytics.monthly_sms_stats_count; 
	create table campaign_analytics.monthly_sms_stats_count as
	select * 
	from 
		(select 
			left(year_month_dd, 7) year_month, 
			
			sum(gp_otp_sms_cnt) gp_otp_sms_cnt, 
			sum(mobireach_otp_sms_cnt) mobireach_otp_sms_cnt, 
			sum(banglalink_otp_sms_cnt) banglalink_otp_sms_cnt, 
			sum(adn_otp_sms_cnt) adn_otp_sms_cnt, 
			
			sum(gp_txn_sms_cnt) gp_txn_sms_cnt, 
			sum(mobireach_txn_sms_cnt) mobireach_txn_sms_cnt, 
			sum(banglalink_txn_sms_cnt) banglalink_txn_sms_cnt, 
			sum(adn_txn_sms_cnt) adn_txn_sms_cnt
		from campaign_analytics.sms_help_b
		group by 1
		) tbl1 
		
		inner join 
		
		(select
			to_char(request_time, 'yyyy-mm') as year_month,
			count(case when telco_identifier_id in(61, 62) then id else null end) as gp_tagada_sms_cnt,
			count(case when telco_identifier_id in(49) then id else null end) as mobireach_tagada_sms_cnt,
			count(case when telco_identifier_id in(64, 66) then id else null end) as banglalink_tagada_sms_cnt, 
			count(case when telco_identifier_id in(67)  then id else null end) as adn_tagada_sms_cnt
		from campaign_analytics.sms_help_c
		where 
			length(mobile_no)=11 
			and mobile_no ~ '^[0-9\.]+$' 
		group by 1 
		) tbl2 using(year_month)
	order by 1; 
	raise notice 'Monthly counts generated'; 
	
	-- monthly statistics (cost)
	drop table if exists campaign_analytics.monthly_sms_stats_cost; 
	create table campaign_analytics.monthly_sms_stats_cost as
	select 
		left(year_month_dd, 7) year_month, 
		
		sum(gp_otp_sms_cnt)*0.2665 gp_otp_sms_cst, 
		sum(mobireach_otp_sms_cnt)*0.1725 mobireach_otp_sms_cst, 
		sum(banglalink_otp_sms_cnt)*0.2399 banglalink_otp_sms_cst, 
		sum(adn_otp_sms_cnt)*0.1575 adn_otp_sms_cst, 
		
		sum(gp_txn_sms_cnt)*0.2665 gp_txn_sms_cst, 
		sum(mobireach_txn_sms_cnt)*0.1725 mobireach_txn_sms_cst, 
		sum(banglalink_txn_sms_cnt)*0.2399 banglalink_txn_sms_cst, 
		sum(adn_txn_sms_cnt)*0.1575 adn_txn_sms_cst
	from campaign_analytics.sms_help_b
	group by 1
	order by 1; 
	raise notice 'Monthly costs generated'; 
	
	-- drop auxiliary tables
	drop table if exists campaign_analytics.sms_help_a; 
	drop table if exists campaign_analytics.sms_help_b; 
	drop table if exists campaign_analytics.sms_help_c; 
		
END;
$function$
;

/*
select campaign_analytics.fn_monthly_sms_stats(); 

select * 
from campaign_analytics.monthly_sms_stats_count; 
select * 
from campaign_analytics.monthly_sms_stats_cost; 
*/
