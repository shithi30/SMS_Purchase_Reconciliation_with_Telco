/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1708749466
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
	1. In Jan, all SPU- how many txn SMS they consumed, distribution of consumption (1-10, 11-20, 21-50, 51-100, 100-150, 150+)
	2. In Jan, distribution of Txn SMS for all users (1-10, 11-20, 21-50, 51-100, 100-150, 150+)
*/

-- tagada SMS
do $$ 

declare 
	var_seq int:=1; 
begin 
	raise notice 'New OP goes below:'; 
	
	-- months for statistics
	drop table if exists data_vajapora.help_c; 
	create table data_vajapora.help_c as
	select *, row_number() over(order by tagada_month asc) seq 
	from 
		(select left(tagada_date::text, 7) tagada_month, max(tagada_date) tagada_month_last_date 
		from 
			(select generate_series(0, current_date-'2021-07-01'::date, 1)+'2021-07-01'::date tagada_date
			) tbl1 
		group by 1
		) tbl1; 
	
	loop 
		delete from data_vajapora.tagada_sms_monthly_distributions
		where year_month=(select tagada_month from data_vajapora.help_c where seq=var_seq);
		delete from data_vajapora.tagada_sms_monthly_distributions_2
		where year_month=(select tagada_month from data_vajapora.help_c where seq=var_seq);
	
		-- tagada SMS of month
		drop table if exists data_vajapora.help_a; 
		create table data_vajapora.help_a as
		select id, mobile_no
		from public.tagada_log 
		where left(create_date::text, 7)=(select tagada_month from data_vajapora.help_c where seq=var_seq);
		
		-- SPUs of month
		drop table if exists data_vajapora.help_b; 
		create table data_vajapora.help_b as
		select distinct mobile_no
		from tallykhata.tk_spu_aspu_data 
		where 
			pu_type='SPU'
			and left(report_date::text, 7)=(select tagada_month from data_vajapora.help_c where seq=var_seq); 
		
		-- PUs of month
		drop table if exists data_vajapora.help_d; 
		create table data_vajapora.help_d as
		select distinct mobile_no 
		from tallykhata.tk_power_users_10 
		where left(report_date::text, 7)=(select tagada_month from data_vajapora.help_c where seq=var_seq);
		
		-- tagada SMS: merchants
		insert into data_vajapora.tagada_sms_monthly_distributions
		select 
			(select tagada_month from data_vajapora.help_c where seq=var_seq) year_month, 
			
			count(tbl1.mobile_no) merchants_consumed_tagada_sms, 
			count(case when tagada_sms_consumed>=1 and tagada_sms_consumed<=10 then tbl1.mobile_no else null end) merchants_consumed_tagada_sms_1_to_10, 
			count(case when tagada_sms_consumed>10 and tagada_sms_consumed<=20 then tbl1.mobile_no else null end) merchants_consumed_tagada_sms_11_to_20, 
			count(case when tagada_sms_consumed>20 and tagada_sms_consumed<=50 then tbl1.mobile_no else null end) merchants_consumed_tagada_sms_21_to_50, 
			count(case when tagada_sms_consumed>50 and tagada_sms_consumed<=100 then tbl1.mobile_no else null end) merchants_consumed_tagada_sms_51_to_100, 
			count(case when tagada_sms_consumed>100 and tagada_sms_consumed<=150 then tbl1.mobile_no else null end) merchants_consumed_tagada_sms_101_to_150, 
			count(case when tagada_sms_consumed>150 then tbl1.mobile_no else null end) merchants_consumed_tagada_sms_more_than_150, 
			
			count(tbl2.mobile_no) spus_consumed_tagada_sms, 
			count(case when tagada_sms_consumed>=1 and tagada_sms_consumed<=10 then tbl2.mobile_no else null end) spus_consumed_tagada_sms_1_to_10, 
			count(case when tagada_sms_consumed>10 and tagada_sms_consumed<=20 then tbl2.mobile_no else null end) spus_consumed_tagada_sms_11_to_20, 
			count(case when tagada_sms_consumed>20 and tagada_sms_consumed<=50 then tbl2.mobile_no else null end) spus_consumed_tagada_sms_21_to_50, 
			count(case when tagada_sms_consumed>50 and tagada_sms_consumed<=100 then tbl2.mobile_no else null end) spus_consumed_tagada_sms_51_to_100, 
			count(case when tagada_sms_consumed>100 and tagada_sms_consumed<=150 then tbl2.mobile_no else null end) spus_consumed_tagada_sms_101_to_150, 
			count(case when tagada_sms_consumed>150 then tbl2.mobile_no else null end) spus_consumed_tagada_sms_more_than_150, 
			
			count(tbl3.mobile_no) pus_consumed_tagada_sms, 
			count(case when tagada_sms_consumed>=1 and tagada_sms_consumed<=10 then tbl3.mobile_no else null end) pus_consumed_tagada_sms_1_to_10, 
			count(case when tagada_sms_consumed>10 and tagada_sms_consumed<=20 then tbl3.mobile_no else null end) pus_consumed_tagada_sms_11_to_20, 
			count(case when tagada_sms_consumed>20 and tagada_sms_consumed<=50 then tbl3.mobile_no else null end) pus_consumed_tagada_sms_21_to_50, 
			count(case when tagada_sms_consumed>50 and tagada_sms_consumed<=100 then tbl3.mobile_no else null end) pus_consumed_tagada_sms_51_to_100, 
			count(case when tagada_sms_consumed>100 and tagada_sms_consumed<=150 then tbl3.mobile_no else null end) pus_consumed_tagada_sms_101_to_150, 
			count(case when tagada_sms_consumed>150 then tbl3.mobile_no else null end) pus_consumed_tagada_sms_more_than_150
		from 
			(select mobile_no, count(id) tagada_sms_consumed
			from data_vajapora.help_a 
			group by 1 
			) tbl1 
			
			left join 
						
			data_vajapora.help_b tbl2 using(mobile_no)
		
			left join
			
			data_vajapora.help_d tbl3 using(mobile_no);
		
		-- tagada SMS: messages
		insert into data_vajapora.tagada_sms_monthly_distributions_2 
		select 
			(select tagada_month from data_vajapora.help_c where seq=var_seq) year_month, 
			
			sum(tagada_sms_consumed) tagada_sms_consumed, 
			sum(case when tagada_sms_consumed>=1 and tagada_sms_consumed<=10 then tagada_sms_consumed else 0 end) tagada_sms_consumed_1_to_10, 
			sum(case when tagada_sms_consumed>10 and tagada_sms_consumed<=20 then tagada_sms_consumed else 0 end) tagada_sms_consumed_11_to_20, 
			sum(case when tagada_sms_consumed>20 and tagada_sms_consumed<=50 then tagada_sms_consumed else 0 end) tagada_sms_consumed_21_to_50, 
			sum(case when tagada_sms_consumed>50 and tagada_sms_consumed<=100 then tagada_sms_consumed else 0 end) tagada_sms_consumed_51_to_100, 
			sum(case when tagada_sms_consumed>100 and tagada_sms_consumed<=150 then tagada_sms_consumed else 0 end) tagada_sms_consumed_101_to_150, 
			sum(case when tagada_sms_consumed>150 then tagada_sms_consumed else 0 end) tagada_sms_consumed_more_than_150, 
			
			sum(case when tbl2.mobile_no is not null then tagada_sms_consumed else 0 end) tagada_sms_consumed_spus, 
			sum(case when tagada_sms_consumed>=1 and tagada_sms_consumed<=10 and tbl2.mobile_no is not null then tagada_sms_consumed else 0 end) tagada_sms_consumed_spus_1_to_10, 
			sum(case when tagada_sms_consumed>10 and tagada_sms_consumed<=20 and tbl2.mobile_no is not null then tagada_sms_consumed else 0 end) tagada_sms_consumed_spus_11_to_20, 
			sum(case when tagada_sms_consumed>20 and tagada_sms_consumed<=50 and tbl2.mobile_no is not null then tagada_sms_consumed else 0 end) tagada_sms_consumed_spus_21_to_50, 
			sum(case when tagada_sms_consumed>50 and tagada_sms_consumed<=100 and tbl2.mobile_no is not null then tagada_sms_consumed else 0 end) tagada_sms_consumed_spus_51_to_100, 
			sum(case when tagada_sms_consumed>100 and tagada_sms_consumed<=150 and tbl2.mobile_no is not null then tagada_sms_consumed else 0 end) tagada_sms_consumed_spus_101_to_150, 
			sum(case when tagada_sms_consumed>150 and tbl2.mobile_no is not null then tagada_sms_consumed else 0 end) tagada_sms_consumed_spus_more_than_150, 
			
			sum(case when tbl3.mobile_no is not null then tagada_sms_consumed else 0 end) tagada_sms_consumed_pus, 
			sum(case when tagada_sms_consumed>=1 and tagada_sms_consumed<=10 and tbl3.mobile_no is not null then tagada_sms_consumed else 0 end) tagada_sms_consumed_pus_1_to_10, 
			sum(case when tagada_sms_consumed>10 and tagada_sms_consumed<=20 and tbl3.mobile_no is not null then tagada_sms_consumed else 0 end) tagada_sms_consumed_pus_11_to_20, 
			sum(case when tagada_sms_consumed>20 and tagada_sms_consumed<=50 and tbl3.mobile_no is not null then tagada_sms_consumed else 0 end) tagada_sms_consumed_pus_21_to_50, 
			sum(case when tagada_sms_consumed>50 and tagada_sms_consumed<=100 and tbl3.mobile_no is not null then tagada_sms_consumed else 0 end) tagada_sms_consumed_pus_51_to_100, 
			sum(case when tagada_sms_consumed>100 and tagada_sms_consumed<=150 and tbl3.mobile_no is not null then tagada_sms_consumed else 0 end) tagada_sms_consumed_pus_101_to_150, 
			sum(case when tagada_sms_consumed>150 and tbl3.mobile_no is not null then tagada_sms_consumed else 0 end) tagada_sms_consumed_pus_more_than_150
		from 
			(select mobile_no, count(id) tagada_sms_consumed
			from data_vajapora.help_a 
			group by 1 
			) tbl1 
			
			left join 
						
			data_vajapora.help_b tbl2 using(mobile_no)
		
			left join
			
			data_vajapora.help_d tbl3 using(mobile_no);
	
		commit; 
		raise notice 'Data generated for: %', (select tagada_month from data_vajapora.help_c where seq=var_seq); 
		var_seq:=var_seq+1; 
		if var_seq=(select max(seq) from data_vajapora.help_c)+1 then exit; 
		end if; 
	end loop; 
end $$; 

select * 
from data_vajapora.tagada_sms_monthly_distributions
order by 1; 

select * 
from data_vajapora.tagada_sms_monthly_distributions_2
order by 1; 

-- txn SMS
do $$ 

declare 
	var_seq int:=1; 
begin 
	raise notice 'New OP goes below:'; 
	
	-- months for statistics
	drop table if exists data_vajapora.help_c; 
	create table data_vajapora.help_c as
	select *, row_number() over(order by txn_sms_month asc) seq 
	from 
		(select left(txn_sms_date::text, 7) txn_sms_month, max(txn_sms_date) txn_sms_month_last_date 
		from 
			(select generate_series(0, current_date-'2021-07-01'::date, 1)+'2021-07-01'::date txn_sms_date
			) tbl1 
		group by 1
		) tbl1; 
	
	loop 
		delete from data_vajapora.txn_sms_monthly_distributions
		where year_month=(select txn_sms_month from data_vajapora.help_c where seq=var_seq); 
		delete from data_vajapora.txn_sms_monthly_distributions_2
		where year_month=(select txn_sms_month from data_vajapora.help_c where seq=var_seq); 
	
		-- tagada/txn SMS of month
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
			and lower(s.message_body) not like '%verification code%'
			and s.telco_identifier_id in(66, 64, 61, 62, 49, 67) 
			and upper(s.message_status) in ('SUCCESS', '0')
			and left(s.request_time::text, 7)=(select txn_sms_month from data_vajapora.help_c where seq=var_seq); 
		
		-- SPUs of month
		drop table if exists data_vajapora.help_b; 
		create table data_vajapora.help_b as
		select distinct mobile_no
		from tallykhata.tk_spu_aspu_data 
		where 
			pu_type='SPU'
			and left(report_date::text, 7)=(select txn_sms_month from data_vajapora.help_c where seq=var_seq); 
		
		-- PUs of month
		drop table if exists data_vajapora.help_d; 
		create table data_vajapora.help_d as
		select distinct mobile_no 
		from tallykhata.tk_power_users_10 
		where left(report_date::text, 7)=(select txn_sms_month from data_vajapora.help_c where seq=var_seq);
	
		-- txn SMS: merchants
		insert into data_vajapora.txn_sms_monthly_distributions
		select 
			(select txn_sms_month from data_vajapora.help_c where seq=var_seq) year_month, 
			
			count(tbl1.mobile_no) merchants_consumed_txn_sms, 
			count(case when txn_sms_consumed>=1 and txn_sms_consumed<=10 then tbl1.mobile_no else null end) merchants_consumed_txn_sms_1_to_10, 
			count(case when txn_sms_consumed>10 and txn_sms_consumed<=20 then tbl1.mobile_no else null end) merchants_consumed_txn_sms_11_to_20, 
			count(case when txn_sms_consumed>20 and txn_sms_consumed<=50 then tbl1.mobile_no else null end) merchants_consumed_txn_sms_21_to_50, 
			count(case when txn_sms_consumed>50 and txn_sms_consumed<=100 then tbl1.mobile_no else null end) merchants_consumed_txn_sms_51_to_100, 
			count(case when txn_sms_consumed>100 and txn_sms_consumed<=150 then tbl1.mobile_no else null end) merchants_consumed_txn_sms_101_to_150, 
			count(case when txn_sms_consumed>150 then tbl1.mobile_no else null end) merchants_consumed_txn_sms_more_than_150, 
			
			count(tbl2.mobile_no) spus_consumed_txn_sms, 
			count(case when txn_sms_consumed>=1 and txn_sms_consumed<=10 then tbl2.mobile_no else null end) spus_consumed_txn_sms_1_to_10, 
			count(case when txn_sms_consumed>10 and txn_sms_consumed<=20 then tbl2.mobile_no else null end) spus_consumed_txn_sms_11_to_20, 
			count(case when txn_sms_consumed>20 and txn_sms_consumed<=50 then tbl2.mobile_no else null end) spus_consumed_txn_sms_21_to_50, 
			count(case when txn_sms_consumed>50 and txn_sms_consumed<=100 then tbl2.mobile_no else null end) spus_consumed_txn_sms_51_to_100, 
			count(case when txn_sms_consumed>100 and txn_sms_consumed<=150 then tbl2.mobile_no else null end) spus_consumed_txn_sms_101_to_150, 
			count(case when txn_sms_consumed>150 then tbl2.mobile_no else null end) spus_consumed_txn_sms_more_than_150, 
			
			count(tbl3.mobile_no) pus_consumed_txn_sms, 
			count(case when txn_sms_consumed>=1 and txn_sms_consumed<=10 then tbl3.mobile_no else null end) pus_consumed_txn_sms_1_to_10, 
			count(case when txn_sms_consumed>10 and txn_sms_consumed<=20 then tbl3.mobile_no else null end) pus_consumed_txn_sms_11_to_20, 
			count(case when txn_sms_consumed>20 and txn_sms_consumed<=50 then tbl3.mobile_no else null end) pus_consumed_txn_sms_21_to_50, 
			count(case when txn_sms_consumed>50 and txn_sms_consumed<=100 then tbl3.mobile_no else null end) pus_consumed_txn_sms_51_to_100, 
			count(case when txn_sms_consumed>100 and txn_sms_consumed<=150 then tbl3.mobile_no else null end) pus_consumed_txn_sms_101_to_150, 
			count(case when txn_sms_consumed>150 then tbl3.mobile_no else null end) pus_consumed_txn_sms_more_than_150
		from 
			(select mobile_no, count(id) txn_sms_consumed
			from data_vajapora.help_a 
			where sms_type!='tagada'
			group by 1 
			) tbl1 
			
			left join 
						
			data_vajapora.help_b tbl2 using(mobile_no)
		
			left join
			
			data_vajapora.help_d tbl3 using(mobile_no);
	
		-- txn SMS: messages
		insert into data_vajapora.txn_sms_monthly_distributions_2 
		select 
			(select txn_sms_month from data_vajapora.help_c where seq=var_seq) year_month, 
			
			sum(txn_sms_consumed) txn_sms_consumed, 
			sum(case when txn_sms_consumed>=1 and txn_sms_consumed<=10 then txn_sms_consumed else 0 end) txn_sms_consumed_1_to_10, 
			sum(case when txn_sms_consumed>10 and txn_sms_consumed<=20 then txn_sms_consumed else 0 end) txn_sms_consumed_11_to_20, 
			sum(case when txn_sms_consumed>20 and txn_sms_consumed<=50 then txn_sms_consumed else 0 end) txn_sms_consumed_21_to_50, 
			sum(case when txn_sms_consumed>50 and txn_sms_consumed<=100 then txn_sms_consumed else 0 end) txn_sms_consumed_51_to_100, 
			sum(case when txn_sms_consumed>100 and txn_sms_consumed<=150 then txn_sms_consumed else 0 end) txn_sms_consumed_101_to_150, 
			sum(case when txn_sms_consumed>150 then txn_sms_consumed else 0 end) txn_sms_consumed_more_than_150, 
			
			sum(case when tbl2.mobile_no is not null then txn_sms_consumed else 0 end) txn_sms_consumed_spus, 
			sum(case when txn_sms_consumed>=1 and txn_sms_consumed<=10 and tbl2.mobile_no is not null then txn_sms_consumed else 0 end) txn_sms_consumed_spus_1_to_10, 
			sum(case when txn_sms_consumed>10 and txn_sms_consumed<=20 and tbl2.mobile_no is not null then txn_sms_consumed else 0 end) txn_sms_consumed_spus_11_to_20, 
			sum(case when txn_sms_consumed>20 and txn_sms_consumed<=50 and tbl2.mobile_no is not null then txn_sms_consumed else 0 end) txn_sms_consumed_spus_21_to_50, 
			sum(case when txn_sms_consumed>50 and txn_sms_consumed<=100 and tbl2.mobile_no is not null then txn_sms_consumed else 0 end) txn_sms_consumed_spus_51_to_100, 
			sum(case when txn_sms_consumed>100 and txn_sms_consumed<=150 and tbl2.mobile_no is not null then txn_sms_consumed else 0 end) txn_sms_consumed_spus_101_to_150, 
			sum(case when txn_sms_consumed>150 and tbl2.mobile_no is not null then txn_sms_consumed else 0 end) txn_sms_consumed_spus_more_than_150, 
			
			sum(case when tbl3.mobile_no is not null then txn_sms_consumed else 0 end) txn_sms_consumed_pus, 
			sum(case when txn_sms_consumed>=1 and txn_sms_consumed<=10 and tbl3.mobile_no is not null then txn_sms_consumed else 0 end) txn_sms_consumed_pus_1_to_10, 
			sum(case when txn_sms_consumed>10 and txn_sms_consumed<=20 and tbl3.mobile_no is not null then txn_sms_consumed else 0 end) txn_sms_consumed_pus_11_to_20, 
			sum(case when txn_sms_consumed>20 and txn_sms_consumed<=50 and tbl3.mobile_no is not null then txn_sms_consumed else 0 end) txn_sms_consumed_pus_21_to_50, 
			sum(case when txn_sms_consumed>50 and txn_sms_consumed<=100 and tbl3.mobile_no is not null then txn_sms_consumed else 0 end) txn_sms_consumed_pus_51_to_100, 
			sum(case when txn_sms_consumed>100 and txn_sms_consumed<=150 and tbl3.mobile_no is not null then txn_sms_consumed else 0 end) txn_sms_consumed_pus_101_to_150, 
			sum(case when txn_sms_consumed>150 and tbl3.mobile_no is not null then txn_sms_consumed else 0 end) txn_sms_consumed_pus_more_than_150
		from 
			(select mobile_no, count(id) txn_sms_consumed
			from data_vajapora.help_a 
			where sms_type!='tagada'
			group by 1 
			) tbl1 
			
			left join 
						
			data_vajapora.help_b tbl2 using(mobile_no)
		
			left join
			
			data_vajapora.help_d tbl3 using(mobile_no);
		
		commit; 
		raise notice 'Data generated for: %', (select txn_sms_month from data_vajapora.help_c where seq=var_seq); 
		var_seq:=var_seq+1; 
		if var_seq=(select max(seq) from data_vajapora.help_c)+1 then exit; 
		end if; 
	end loop; 
end $$; 

select * 
from data_vajapora.txn_sms_monthly_distributions
order by 1; 

select * 
from data_vajapora.txn_sms_monthly_distributions_2
order by 1; 

-- txn SMS: breakdown of users who consumed>150 txn SMS in a month
do $$ 

declare 
	var_seq int:=1; 
begin 
	raise notice 'New OP goes below:'; 
	
	-- months for statistics
	drop table if exists data_vajapora.help_c; 
	create table data_vajapora.help_c as
	select *, row_number() over(order by txn_sms_month asc) seq 
	from 
		(select left(txn_sms_date::text, 7) txn_sms_month, max(txn_sms_date) txn_sms_month_last_date 
		from 
			(select generate_series(0, current_date-'2021-07-01'::date, 1)+'2021-07-01'::date txn_sms_date
			) tbl1 
		group by 1
		) tbl1; 
	
	loop 
		delete from data_vajapora.txn_sms_monthly_distributions_huge
		where year_month=(select txn_sms_month from data_vajapora.help_c where seq=var_seq); 
	
		-- txn SMS of month
		drop table if exists data_vajapora.help_a; 
		create table data_vajapora.help_a as
		select id, translate(substring(message_body, length(message_body)-10, 12), '০১২৩৪৫৬৭৮৯', '0123456789') mobile_no
		from public.t_scsms_message_archive_v2 as s
		where
			upper(s.channel) in('TALLYKHATA_TXN') 
			and upper(trim(s.bank_name)) = 'SURECASH'
			and lower(s.message_body) not like '%verification code%'
			and message_body not like '%অনুগ্রহ করে%'
			and s.telco_identifier_id in(66, 64, 61, 62, 49, 67) 
			and upper(s.message_status) in ('SUCCESS', '0')
			and left(s.request_time::text, 7)=(select txn_sms_month from data_vajapora.help_c where seq=var_seq); 
		
		-- txn SMS: merchants, messages
		insert into data_vajapora.txn_sms_monthly_distributions_huge
		select 
			(select txn_sms_month from data_vajapora.help_c where seq=var_seq) year_month, 
			
			count(tbl1.mobile_no) merchants_consumed_txn_sms, 
			count(case when txn_sms_consumed>150 and txn_sms_consumed<=200 then tbl1.mobile_no else null end) merchants_consumed_txn_sms_151_to_200, 
			count(case when txn_sms_consumed>200 and txn_sms_consumed<=250 then tbl1.mobile_no else null end) merchants_consumed_txn_sms_201_to_250, 
			count(case when txn_sms_consumed>250 and txn_sms_consumed<=300 then tbl1.mobile_no else null end) merchants_consumed_txn_sms_251_to_300, 
			count(case when txn_sms_consumed>300 and txn_sms_consumed<=350 then tbl1.mobile_no else null end) merchants_consumed_txn_sms_301_to_350, 
			count(case when txn_sms_consumed>350 and txn_sms_consumed<=400 then tbl1.mobile_no else null end) merchants_consumed_txn_sms_351_to_400, 
			count(case when txn_sms_consumed>400 then tbl1.mobile_no else null end) merchants_consumed_txn_sms_more_than_400, 
			
			sum(txn_sms_consumed) txn_sms_consumed, 
			sum(case when txn_sms_consumed>150 and txn_sms_consumed<=200 then txn_sms_consumed else 0 end) txn_sms_consumed_151_to_200, 
			sum(case when txn_sms_consumed>200 and txn_sms_consumed<=250 then txn_sms_consumed else 0 end) txn_sms_consumed_201_to_250, 
			sum(case when txn_sms_consumed>250 and txn_sms_consumed<=300 then txn_sms_consumed else 0 end) txn_sms_consumed_251_to_300, 
			sum(case when txn_sms_consumed>300 and txn_sms_consumed<=350 then txn_sms_consumed else 0 end) txn_sms_consumed_301_to_350, 
			sum(case when txn_sms_consumed>350 and txn_sms_consumed<=400 then txn_sms_consumed else 0 end) txn_sms_consumed_351_to_400, 
			sum(case when txn_sms_consumed>400 then txn_sms_consumed else 0 end) txn_sms_consumed_more_than_400
		from 
			(select mobile_no, count(id) txn_sms_consumed
			from data_vajapora.help_a 
			group by 1 
			having count(id)>150
			) tbl1; 
		
		commit; 
		raise notice 'Data generated for: %', (select txn_sms_month from data_vajapora.help_c where seq=var_seq); 
		var_seq:=var_seq+1; 
		if var_seq=(select max(seq) from data_vajapora.help_c)+1 then exit; 
		end if; 
	end loop; 
end $$; 

select * 
from data_vajapora.txn_sms_monthly_distributions_huge
order by 1; 
