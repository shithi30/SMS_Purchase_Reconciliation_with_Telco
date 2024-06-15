/*
- Viz: 
	- https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=507917107
	- https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=2091636547
	- https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=287838140
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation: https://docs.google.com/presentation/d/1KeKjbSasRnl1T0oXPj-m1EwHlZRtoT764AVL3d7z4cg/edit#slide=id.ge62f3976a2_0_0
- Email thread: Internet and SMS Usages Stats of TallyKhata
- Notes (if any): 
*/

-- tagada + txn
do $$

declare
	var_date date:=current_date-30; 
begin 
	
	delete from data_vajapora.daily_sms_consumption_info
	where date>=var_date;

	raise notice 'New OP goes below:'; 

	loop
		drop table if exists data_vajapora.help_a;
		create table data_vajapora.help_a as
		select var_date date, merchant_mobile, count(id) sms_consumed
		from 
			(select id, translate(substring(message_body, length(message_body)-10, 12), '০১২৩৪৫৬৭৮৯', '0123456789') merchant_mobile
			from public.t_scsms_message_archive_v2
			where 
				date(request_time)=var_date
				and message_status in('SUCCESS')
			) tbl1 
		group by 1, 2; 
	
		insert into data_vajapora.daily_sms_consumption_info
		select 
			date,
			avg(case when dau_mobile_no is not null then sms_consumed else null end) avg_dau_sms_consumption, 
			avg(case when pu_mobile_no is not null then sms_consumed else null end) avg_pu_sms_consumption, 
			avg(case when rau3_mobile_no is not null then sms_consumed else null end) avg_rau3_sms_consumption
		from 
			data_vajapora.help_a tbl1 
			
			left join 
			
			(select distinct mobile_no pu_mobile_no 
			from tallykhata.tk_power_users_10 
			where report_date=var_date
			) tbl2 on(tbl1.merchant_mobile=tbl2.pu_mobile_no)
			
			left join 
			
			(select mobile_no dau_mobile_no 
			from tallykhata.tallykhata_user_date_sequence_final 
			where event_date=var_date
			) tbl3 on(tbl1.merchant_mobile=tbl3.dau_mobile_no)
			
			left join 
			
			(select mobile_no rau3_mobile_no
			from tallykhata.tallykhata_regular_active_user 
			where 
				rau_category=3
				and rau_date=var_date
			) tbl4 on(tbl1.merchant_mobile=tbl4.rau3_mobile_no)
		group by 1; 
		
		raise notice 'Data generated for: %', var_date; 
	
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if;

	end loop; 

end $$; 

/*	
truncate table data_vajapora.daily_sms_consumption_info; 

select 
	*, 
	ceil(avg_dau_sms_consumption) avg_dau_sms_consumption_rounded,
	ceil(avg_pu_sms_consumption) avg_pu_sms_consumption_rounded,
	ceil(avg_rau3_sms_consumption) avg_rau3_sms_consumption_rounded
from data_vajapora.daily_sms_consumption_info; 
*/

-- tagada
do $$

declare
	var_date date:=current_date-30; 
begin 
	
	delete from data_vajapora.daily_sms_consumption_info_tagada
	where date>=var_date;

	raise notice 'New OP goes below:'; 

	loop
		drop table if exists data_vajapora.help_a;
		create table data_vajapora.help_a as
		select var_date date, merchant_mobile, count(id) sms_consumed
		from 
			(select id, translate(substring(message_body, length(message_body)-10, 12), '০১২৩৪৫৬৭৮৯', '0123456789') merchant_mobile
			from public.t_scsms_message_archive_v2
			where 
				date(request_time)=var_date
				and message_status in('SUCCESS')
				and left(message_body, 9)='মোট বাকি '
			) tbl1 
		group by 1, 2; 
	
		insert into data_vajapora.daily_sms_consumption_info_tagada
		select 
			date,
			avg(case when dau_mobile_no is not null then sms_consumed else null end) avg_dau_sms_consumption, 
			avg(case when pu_mobile_no is not null then sms_consumed else null end) avg_pu_sms_consumption, 
			avg(case when rau3_mobile_no is not null then sms_consumed else null end) avg_rau3_sms_consumption
		from 
			data_vajapora.help_a tbl1 
			
			left join 
			
			(select distinct mobile_no pu_mobile_no 
			from tallykhata.tk_power_users_10 
			where report_date=var_date
			) tbl2 on(tbl1.merchant_mobile=tbl2.pu_mobile_no)
			
			left join 
			
			(select mobile_no dau_mobile_no 
			from tallykhata.tallykhata_user_date_sequence_final 
			where event_date=var_date
			) tbl3 on(tbl1.merchant_mobile=tbl3.dau_mobile_no)
			
			left join 
			
			(select mobile_no rau3_mobile_no
			from tallykhata.tallykhata_regular_active_user 
			where 
				rau_category=3
				and rau_date=var_date
			) tbl4 on(tbl1.merchant_mobile=tbl4.rau3_mobile_no)
		group by 1; 
		
		raise notice 'Data generated for: %', var_date; 
	
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if;

	end loop; 

end $$; 

/*	
truncate table data_vajapora.daily_sms_consumption_info_tagada; 

select 
	*, 
	ceil(avg_dau_sms_consumption) avg_dau_sms_consumption_rounded,
	ceil(avg_pu_sms_consumption) avg_pu_sms_consumption_rounded,
	ceil(avg_rau3_sms_consumption) avg_rau3_sms_consumption_rounded
from data_vajapora.daily_sms_consumption_info_tagada; 
*/

-- txn
do $$

declare
	var_date date:=current_date-30; 
begin 
	
	delete from data_vajapora.daily_sms_consumption_info_txn
	where date>=var_date;

	raise notice 'New OP goes below:'; 

	loop
		drop table if exists data_vajapora.help_a;
		create table data_vajapora.help_a as
		select var_date date, merchant_mobile, count(id) sms_consumed
		from 
			(select id, translate(substring(message_body, length(message_body)-10, 12), '০১২৩৪৫৬৭৮৯', '0123456789') merchant_mobile
			from public.t_scsms_message_archive_v2
			where 
				date(request_time)=var_date
				and message_status in('SUCCESS')
				and left(message_body, 9)!='মোট বাকি '
			) tbl1 
		group by 1, 2; 
	
		insert into data_vajapora.daily_sms_consumption_info_txn
		select 
			date,
			avg(case when dau_mobile_no is not null then sms_consumed else null end) avg_dau_sms_consumption, 
			avg(case when pu_mobile_no is not null then sms_consumed else null end) avg_pu_sms_consumption, 
			avg(case when rau3_mobile_no is not null then sms_consumed else null end) avg_rau3_sms_consumption
		from 
			data_vajapora.help_a tbl1 
			
			left join 
			
			(select distinct mobile_no pu_mobile_no 
			from tallykhata.tk_power_users_10 
			where report_date=var_date
			) tbl2 on(tbl1.merchant_mobile=tbl2.pu_mobile_no)
			
			left join 
			
			(select mobile_no dau_mobile_no 
			from tallykhata.tallykhata_user_date_sequence_final 
			where event_date=var_date
			) tbl3 on(tbl1.merchant_mobile=tbl3.dau_mobile_no)
			
			left join 
			
			(select mobile_no rau3_mobile_no
			from tallykhata.tallykhata_regular_active_user 
			where 
				rau_category=3
				and rau_date=var_date
			) tbl4 on(tbl1.merchant_mobile=tbl4.rau3_mobile_no)
		group by 1; 
		
		raise notice 'Data generated for: %', var_date; 
	
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if;

	end loop; 

end $$; 

/*	
truncate table data_vajapora.daily_sms_consumption_info_txn; 

select 
	*, 
	ceil(avg_dau_sms_consumption) avg_dau_sms_consumption_rounded,
	ceil(avg_pu_sms_consumption) avg_pu_sms_consumption_rounded,
	ceil(avg_rau3_sms_consumption) avg_rau3_sms_consumption_rounded
from data_vajapora.daily_sms_consumption_info_txn; 
*/

