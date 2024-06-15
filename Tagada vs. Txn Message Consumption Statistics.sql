/*
- Viz: 
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=705877143
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=639814240
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
*/

/*
Current SMS # breakdown 
>>Txn SMS concountption distribution (0-20, 21-50, 51-100, 100+)
*/

do $$ 

declare 
	var_date date:=current_date-33; 
begin 
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.sms_consumption_distribution 
		where report_date=var_date; 
	
		drop table if exists data_vajapora.help_b; 
		create table data_vajapora.help_b as
		select translate(substring(message_body, length(message_body)-10, 12), '০১২৩৪৫৬৭৮৯', '0123456789') mobile_no, count(id) txn_sms_consumed
		from public.t_scsms_message_archive_v2 as s
		where
			upper(s.channel) in('TALLYKHATA_TXN') 
			and upper(trim(s.bank_name)) = 'SURECASH'
			and s.telco_identifier_id in(66, 64, 61, 62, 49) 
			and upper(s.message_status) in ('SUCCESS', '0')
			and message_body not like '%অনুগ্রহ করে%'
			and s.request_time::date>=var_date-30 and s.request_time::date<var_date
		group by 1; 
		
		insert into data_vajapora.sms_consumption_distribution
		select 
			var_date report_date, 
			count(mobile_no) txn_sms_consumed, 
			count(case when txn_sms_consumed<=20 then mobile_no else null end) txn_sms_consumed_1_to_20,
			count(case when txn_sms_consumed>20 and txn_sms_consumed<=50 then mobile_no else null end) txn_sms_consumed_21_to_50,
			count(case when txn_sms_consumed>50 and txn_sms_consumed<=100 then mobile_no else null end) txn_sms_consumed_51_to_100, 
			count(case when txn_sms_consumed>100 then mobile_no else null end) txn_sms_consumed_more_than_100
		from data_vajapora.help_b;

		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date-18 then exit; 
		end if;
	end loop; 
end $$; 

select *
from data_vajapora.sms_consumption_distribution
order by 1;

/*
Current SMS # breakdown 
>>Txn SMS % vs. Tagada SMS % 
>>Usage by SPUs: TXN, Tagada 
*/

do $$ 

declare 
	var_date date:='2022-01-01'::date; 
begin 
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.tagada_vs_txn_sms_stats 
		where report_date=var_date; 
	
		drop table if exists data_vajapora.help_a; 
		create table data_vajapora.help_a as
		select 
			id, 
			translate(substring(message_body, length(message_body)-10, 12), '০১২৩৪৫৬৭৮৯', '0123456789') mobile_no, 
			case 
				when message_body like '%অনুগ্রহ করে%' then 'tagada'
				when message_body like '%বাকি%' and message_body like 'প্রিয় গ্রাহক%' then 'baki add'
				when message_body like '%বাকি%' and (message_body like 'পরিশোধ%' or message_body like 'দিলাম%' or message_body like 'কেনা%') then 'baki txn'
				else 'other txn'
			end sms_type
		from public.t_scsms_message_archive_v2 as s
		where
			upper(s.channel) in('TALLYKHATA_TXN') 
			and upper(trim(s.bank_name)) = 'SURECASH'
			and s.telco_identifier_id in(66, 64, 61, 62, 49) 
			and upper(s.message_status) in ('SUCCESS', '0')
			and s.request_time::date=var_date; 
		
		insert into data_vajapora.tagada_vs_txn_sms_stats
		select 
			var_date report_date, 
			
			count(case when sms_type in('tagada') then id else null end) tagada_sms_consumed, 
			count(case when sms_type in('baki add', 'baki txn') then id else null end) baki_sms_consumed, 
			count(case when sms_type in('baki add', 'baki txn', 'other txn') then id else null end) txn_sms_consumed, 
			
			count(distinct case when tbl2.mobile_no is not null and sms_type in('tagada') then mobile_no else null end) spus_consumed_tagada_sms, 
			count(distinct case when tbl2.mobile_no is not null and sms_type in('baki add', 'baki txn') then mobile_no else null end) spus_consumed_baki_sms, 
			count(distinct case when tbl2.mobile_no is not null and sms_type in('baki add', 'baki txn', 'other txn') then mobile_no else null end) spus_consumed_txn_sms 
		from 
			data_vajapora.help_a tbl1 
			
			left join 
			
			(select mobile_no
			from tallykhata.tk_spu_aspu_data 
			where 
				pu_type='SPU'
				and report_date=var_date
			) tbl2 using(mobile_no); 
	
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if;
	end loop; 
end $$; 

select *
from data_vajapora.tagada_vs_txn_sms_stats;

/*
-- sanity check
select report_date, count(distinct mobile_no) merchants_consumed_tagada
from
	(select merchant_mobile mobile_no, date report_date
	from public.notification_tagadasms 
	where date>=current_date-30 and date<current_date
	) tbl1 
	
	inner join 
	
	(select mobile_no, report_date
	from tallykhata.tk_spu_aspu_data 
	where pu_type='SPU'
	) tbl2 using(mobile_no, report_date)
group by 1
order by 1; 
*/
