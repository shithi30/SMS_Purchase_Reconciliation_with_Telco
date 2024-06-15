/*
- Viz: 
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1650662495
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=296057273
- Data: 
- Function: 
- Table:
- Instructions: https://docs.google.com/spreadsheets/d/1xkrMVoL6fx9TX1j9LJHRbbENF9gn2cC6VGmFcGvfBZE/edit#gid=0
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
*/

do $$ 

declare 
	var_seq int:=1; 
begin 
	raise notice 'New OP goes below:'; 
	
	drop table if exists data_vajapora.help_a; 
	create table data_vajapora.help_a as
	select *, row_number() over(order by tagada_month asc) seq 
	from 
		(select left(tagada_date::text, 7) tagada_month, max(tagada_date) tagada_month_last_date 
		from 
			(select generate_series(0, current_date-'2021-07-01'::date, 1)+'2021-07-01'::date tagada_date
			) tbl1 
		group by 1
		) tbl1; 
	
	loop 
		delete from data_vajapora.tagada_monthly_stats 
		where year_month=(select tagada_month from data_vajapora.help_a where seq=var_seq); 
	
		insert into data_vajapora.tagada_monthly_stats 
		select 
			(select tagada_month from data_vajapora.help_a where seq=var_seq) year_month, 
			
			count(distinct tbl1.mobile_no) merchants_used_tagada, 
			count(distinct tbl1.account_id) customers_got_tagada, 
			
			count(distinct case when reg_year_month=(select tagada_month from data_vajapora.help_a where seq=var_seq) then tbl1.mobile_no else null end) new_registered_used_tagada, 
			count(distinct case when reg_year_month!=(select tagada_month from data_vajapora.help_a where seq=var_seq) then tbl1.mobile_no else null end) old_registered_used_tagada, 
			
			count(distinct case when tbl2.mobile_no is not null then tbl1.mobile_no else null end) merchants_used_tagada_spus, 
			count(distinct case when tbl2.mobile_no is null and tbl3.mobile_no is not null then tbl1.mobile_no else null end) merchants_used_tagada_3raus, 
			count(distinct case when tbl2.mobile_no is null and tbl3.mobile_no is null and tbl4.mobile_no is not null then tbl1.mobile_no else null end) merchants_used_tagada_pus, 
			count(distinct case when tbl2.mobile_no is null and tbl3.mobile_no is null and tbl4.mobile_no is null then tbl1.mobile_no else null end) merchants_used_tagada_other_segment,                  
			
			count(distinct case when bi_business_type in('Grocery Business') then tbl1.mobile_no else null end) merchants_used_tagada_grocery, 
			count(distinct case when bi_business_type in('Pharmacy Business') then tbl1.mobile_no else null end) merchants_used_tagada_pharmacy, 
			count(distinct case when bi_business_type in('Electronics service (mobile service/tv, fan service/boring center/online services)+recharge Business') then tbl1.mobile_no else null end) merchants_used_tagada_electronics, 
			count(distinct case when bi_business_type in('Other wholesaler goods/services business') then tbl1.mobile_no else null end) merchants_used_tagada_wholesaler, 
			count(distinct case when bi_business_type not in('Grocery Business', 'Pharmacy Business', 'Electronics service (mobile service/tv, fan service/boring center/online services)+recharge Business', 'Other wholesaler goods/services business') then tbl1.mobile_no else null end) merchants_used_tagada_other_types                                               
		from 
			(-- used Tagada
			select mobile_no, account_id
			from public.tagada_log 
			where left(create_date::text, 7)=(select tagada_month from data_vajapora.help_a where seq=var_seq)
			) tbl1 
			
			left join 
			
			(-- SPU
			select mobile_no
			from tallykhata.tk_spu_aspu_data 
			where 
				pu_type='SPU'
				and report_date=(select tagada_month_last_date from data_vajapora.help_a where seq=var_seq)
			) tbl2 using(mobile_no)
			
			left join 
			
			(-- 3RAU
			select mobile_no
			from tallykhata.regular_active_user_event
			where 
				rau_category=3 
				and report_date::date=(select tagada_month_last_date from data_vajapora.help_a where seq=var_seq)
			) tbl3 using(mobile_no)
			
			left join 
			
			(-- PU
			select mobile_no 
			from tallykhata.tk_power_users_10
			where report_date=(select tagada_month_last_date from data_vajapora.help_a where seq=var_seq)
			) tbl4 using(mobile_no)
			
			left join 
			
			(-- reg info.
			select mobile_number mobile_no, left(created_at::text, 7) reg_year_month 
			from public.register_usermobile 
			) tbl5 using(mobile_no)
				
			left join 
		
			(-- BI types
			select mobile mobile_no, max(bi_business_type) bi_business_type
			from tallykhata.tallykhata_user_personal_info 
			group by 1
			) tbl6 using(mobile_no);
	
		commit; 
		raise notice 'Data generated for: %', (select tagada_month from data_vajapora.help_a where seq=var_seq); 
		var_seq:=var_seq+1; 
		if var_seq=(select max(seq) from data_vajapora.help_a)+1 then exit; 
		end if; 
	end loop; 
end $$; 

select *
from data_vajapora.tagada_monthly_stats; 

select 
	year_month, 
	-- count(mobile_no) merchants_consumed_tagada, 
	count(case when tagada_consumed>=1 and tagada_consumed<=20 then mobile_no else null end) tagada_consumed_1_to_20,
	count(case when tagada_consumed>20 and tagada_consumed<=40 then mobile_no else null end) tagada_consumed_21_to_40,
	count(case when tagada_consumed>40 and tagada_consumed<=60 then mobile_no else null end) tagada_consumed_41_to_60,
	count(case when tagada_consumed>60 and tagada_consumed<=80 then mobile_no else null end) tagada_consumed_61_to_80,
	count(case when tagada_consumed>80 and tagada_consumed<=100 then mobile_no else null end) tagada_consumed_81_to_100,
	count(case when tagada_consumed>100 then mobile_no else null end) tagada_consumed_more_than_100
from 
	(select 
		left(create_date::text, 7) year_month, 
		mobile_no, 
		count(*) tagada_consumed
	from public.tagada_log 
	group by 1, 2
	) tbl1
group by 1; 

-- free, own, share
select
	left(create_date::text, 7) year_month, 
	
	-- count(id) tagada_sms,
	count(case when tagada_type='TAGADA_BY_SMS' then id else null end) tagada_sms_own, 
	count(case when tagada_type='TAGADA_BY_FREE_SMS' then id else null end) tagada_sms_quota, 
	count(case when tagada_type='TAGADA_BY_SHARE' then id else null end) tagada_sms_share, 
	
	count(distinct case when tagada_type='TAGADA_BY_SMS' then mobile_no else null end) tagada_merchants_own, 
	count(distinct case when tagada_type='TAGADA_BY_FREE_SMS' then mobile_no else null end) tagada_merchants_quota, 
	count(distinct case when tagada_type='TAGADA_BY_SHARE' then mobile_no else null end) tagada_merchants_share 
from public.tagada_log 
group by 1; 

