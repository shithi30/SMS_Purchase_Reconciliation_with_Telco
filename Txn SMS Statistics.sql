/*
- Viz: 
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=144552136
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1298384198
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1809707183
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1496854736
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
	Would like to understand TXN SMS usage. Examples: How many per day and month. 
	TXN to SMS ratio. Usage distribution by user types etc. Monthly expenses.
	
	Txn SMS (not Tagada or OTP) consumption tendencies have been analyzed. Findings: 
	- ~ 3 lac txn SMS are consumed daily
	- ~ 83 lac txn SMS are consumed monthly 
	- SPUs, PUs, 3RAUs and LTUs consume 75%, 15%, 2% and 2% txn SMS respectively
	- In general, 28% transactions lead to a subsequent SMS daily. 
	- SPU: 33%, PU: 21%, 3RAU: 18%, LTU: 16%, Zombie+NN: 20%, Personal: 13%
	- We incur a daily cost of 75k BDT and a monthly cost of 20lac BDT for txn SMS. 75% of this expense is due to the SPUs. 
*/

-- SMS distribution
do $$

declare 
	var_date date:=current_date-3; 
begin  
	raise notice 'New OP goes below:'; 

	drop table if exists data_vajapora.temp_a; 
	create table data_vajapora.temp_a as
	select id, translate(substring(message_body, length(message_body)-10, 12), '০১২৩৪৫৬৭৮৯', '0123456789') mobile_no, request_time, telco_identifier_id
	from public.t_scsms_message_archive_v2
	where
		1=1
		and channel='TALLYKHATA_TXN'
		and bank_name='SURECASH'
		and lower(message_body) not like '%verification code%'
		and message_body not like '%অনুগ্রহ করে%'
		and telco_identifier_id in(66, 64, 61, 62, 49, 67) 
		and upper(message_status) in('SUCCESS', '0'); 
	raise notice 'Txn SMSs extracted.'; 
	
	loop
		delete from data_vajapora.txn_sms_stats_1
		where report_date=var_date;
	
		drop table if exists data_vajapora.temp_b; 
		create table data_vajapora.temp_b as
		select 
			mobile_no, 
			case 
				when tg in('SPU') or mobile_no in(select mobile_no from tallykhata.tk_spu_aspu_data where pu_type in('SPU', 'Sticky SPU') and report_date=var_date) then 'SPU'
				when tg ilike 'pu%' then 'PUAll'
				when tg ilike '3rau%'  then '3RAUAll'
				when tg ilike 'ltu%' then 'LTUAll' 
				when tg ilike 'z%' then 'ZAll'
				when tg ilike 'psu%' then 'PSU'
				when tg in('NN2-6', 'NN1', 'NB0') then 'NN'
				when tg ilike '%NT%' then 'NT' 
			end tg_shrunk
		from cjm_segmentation.retained_users 
		where report_date=var_date; 
		
		insert into data_vajapora.txn_sms_stats_1
		select 
			var_date report_date, 
			count(id) txn_sms_consumed, 
			count(case when tg_shrunk='SPU' then id else null end) txn_sms_consumed_SPU, 
			count(case when tg_shrunk='PUAll' then id else null end) txn_sms_consumed_PUAll, 
			count(case when tg_shrunk='3RAUAll' then id else null end) txn_sms_consumed_3RAUAll, 
			count(case when tg_shrunk='LTUAll' then id else null end) txn_sms_consumed_LTUAll, 
			count(case when tg_shrunk='ZAll' then id else null end) txn_sms_consumed_ZAll, 
			count(case when tg_shrunk='PSU' then id else null end) txn_sms_consumed_PSU, 
			count(case when tg_shrunk='NN' then id else null end) txn_sms_consumed_NN, 
			count(case when tg_shrunk='NT' then id else null end) txn_sms_consumed_NT, 
			count(case when tg_shrunk is null then id else null end) txn_sms_consumed_rest 
		from 
			(select id, mobile_no 
			from data_vajapora.temp_a
			where date(request_time)=var_date
			) tbl1 
			
			left join 
		
			data_vajapora.temp_b tbl2 using(mobile_no); 
		
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

select * 
from data_vajapora.txn_sms_stats_1; 

select 
	left(report_date::text, 7) report_month, 
	sum(txn_sms_consumed) txn_sms_consumed,
	sum(txn_sms_consumed_spu) txn_sms_consumed_spu,
	sum(txn_sms_consumed_puall) txn_sms_consumed_puall,
	sum(txn_sms_consumed_3rauall) txn_sms_consumed_3rauall,
	sum(txn_sms_consumed_ltuall) txn_sms_consumed_ltuall,
	sum(txn_sms_consumed_zall) txn_sms_consumed_zall,
	sum(txn_sms_consumed_psu) txn_sms_consumed_psu,
	sum(txn_sms_consumed_nn) txn_sms_consumed_nn,
	sum(txn_sms_consumed_nt) txn_sms_consumed_nt,
	sum(txn_sms_consumed_rest) txn_sms_consumed_rest
from data_vajapora.txn_sms_stats_1
group by 1
order by 1; 

-- txn distribution
do $$

declare 
	var_date date:='2022-02-05'::date; 
begin  
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.txn_sms_stats_2
		where report_date=var_date;
	
		drop table if exists data_vajapora.help_b; 
		create table data_vajapora.help_b as
		select 
			mobile_no, 
			case 
				when tg in('SPU') or mobile_no in(select mobile_no from tallykhata.tk_spu_aspu_data where pu_type in('SPU', 'Sticky SPU') and report_date=var_date) then 'SPU'
				when tg ilike 'pu%' then 'PUAll'
				when tg ilike '3rau%'  then '3RAUAll'
				when tg ilike 'ltu%' then 'LTUAll' 
				when tg ilike 'z%' then 'ZAll'
				when tg ilike 'psu%' then 'PSU'
				when tg in('NN2-6', 'NN1', 'NB0') then 'NN'
				when tg ilike '%NT%' then 'NT' 
			end tg_shrunk
		from cjm_segmentation.retained_users 
		where report_date=var_date; 
		
		insert into data_vajapora.txn_sms_stats_2
		select 
			var_date report_date, 
			count(auto_id) txns, 
			count(case when tg_shrunk='SPU' then auto_id else null end) txns_SPU, 
			count(case when tg_shrunk='PUAll' then auto_id else null end) txns_PUAll, 
			count(case when tg_shrunk='3RAUAll' then auto_id else null end) txns_3RAUAll, 
			count(case when tg_shrunk='LTUAll' then auto_id else null end) txns_LTUAll, 
			count(case when tg_shrunk='ZAll' then auto_id else null end) txns_ZAll, 
			count(case when tg_shrunk='PSU' then auto_id else null end) txns_PSU, 
			count(case when tg_shrunk='NN' then auto_id else null end) txns_NN, 
			count(case when tg_shrunk='NT' then auto_id else null end) txns_NT, 
			count(case when tg_shrunk is null then auto_id else null end) txns_rest 
		from 
			(select auto_id, mobile_no 
			from tallykhata.tallykhata_fact_info_final
			where created_datetime=var_date
			) tbl1 
			
			left join 
		
			data_vajapora.help_b tbl2 using(mobile_no);
			
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

select * 
from data_vajapora.txn_sms_stats_2; 

-- txn-to-SMS ratio
select 
	report_date,
	txn_sms_consumed, txns, txn_sms_consumed*1.00/txns txn_to_sms_ratio,
	txn_sms_consumed_spu, txns_spu, txn_sms_consumed_spu*1.00/txns_spu txn_to_sms_ratio_spu, 
	txn_sms_consumed_puall, txns_puall, txn_sms_consumed_puall*1.00/txns_puall txn_to_sms_ratio_puall, 
	txn_sms_consumed_3rauall, txns_3rauall, txn_sms_consumed_3rauall*1.00/txns_3rauall txn_to_sms_ratio_3rauall, 
	txn_sms_consumed_ltuall, txns_ltuall, txn_sms_consumed_ltuall*1.00/txns_ltuall txn_to_sms_ratio_ltuall, 
	txn_sms_consumed_zall, txns_zall, txn_sms_consumed_zall*1.00/txns_zall txn_to_sms_ratio_zall, 
	txn_sms_consumed_psu, txns_psu, txn_sms_consumed_psu*1.00/txns_psu txn_to_sms_ratio_psu, 
	txn_sms_consumed_nn, txns_nn, txn_sms_consumed_nn*1.00/txns_nn txn_to_sms_ratio_nn, 
	txn_sms_consumed_nt, txns_nt, txn_sms_consumed_nt*1.00/txns_nt txn_to_sms_ratio_nt, 
	txn_sms_consumed_rest, txns_rest, txn_sms_consumed_rest*1.00/txns_rest txn_to_sms_ratio_rest
from 
	data_vajapora.txn_sms_stats_1 tbl1 
	inner join 
	data_vajapora.txn_sms_stats_2 tbl2 using(report_date)
order by 1; 

-- txn-to-SMS ratio (summary)
select 
	avg(txn_to_sms_ratio) txn_to_sms_ratio, 
	avg(txn_to_sms_ratio_spu) txn_to_sms_ratio_spu, 
	avg(txn_to_sms_ratio_puall) txn_to_sms_ratio_puall, 
	avg(txn_to_sms_ratio_3rauall) txn_to_sms_ratio_3rauall, 
	avg(txn_to_sms_ratio_ltuall) txn_to_sms_ratio_ltuall, 
	avg(txn_to_sms_ratio_zall) txn_to_sms_ratio_zall, 
	avg(txn_to_sms_ratio_psu) txn_to_sms_ratio_psu, 
	avg(txn_to_sms_ratio_nn) txn_to_sms_ratio_nn, 
	avg(txn_to_sms_ratio_nt) txn_to_sms_ratio_nt, 
	avg(txn_to_sms_ratio_rest) txn_to_sms_ratio_rest
from 
	(select 
		report_date,
		txn_sms_consumed, txns, txn_sms_consumed*1.00/txns txn_to_sms_ratio,
		txn_sms_consumed_spu, txns_spu, txn_sms_consumed_spu*1.00/txns_spu txn_to_sms_ratio_spu, 
		txn_sms_consumed_puall, txns_puall, txn_sms_consumed_puall*1.00/txns_puall txn_to_sms_ratio_puall, 
		txn_sms_consumed_3rauall, txns_3rauall, txn_sms_consumed_3rauall*1.00/txns_3rauall txn_to_sms_ratio_3rauall, 
		txn_sms_consumed_ltuall, txns_ltuall, txn_sms_consumed_ltuall*1.00/txns_ltuall txn_to_sms_ratio_ltuall, 
		txn_sms_consumed_zall, txns_zall, txn_sms_consumed_zall*1.00/txns_zall txn_to_sms_ratio_zall, 
		txn_sms_consumed_psu, txns_psu, txn_sms_consumed_psu*1.00/txns_psu txn_to_sms_ratio_psu, 
		txn_sms_consumed_nn, txns_nn, txn_sms_consumed_nn*1.00/txns_nn txn_to_sms_ratio_nn, 
		txn_sms_consumed_nt, txns_nt, txn_sms_consumed_nt*1.00/txns_nt txn_to_sms_ratio_nt, 
		txn_sms_consumed_rest, txns_rest, txn_sms_consumed_rest*1.00/txns_rest txn_to_sms_ratio_rest, 
		row_number() over(order by report_date desc) seq
	from 
		data_vajapora.txn_sms_stats_1 tbl1 
		inner join 
		data_vajapora.txn_sms_stats_2 tbl2 using(report_date)
	) tbl1
where seq<16; 

/*
-- taking infinite time for date: 2022-03-11
select 
	mobile_no, 
	case 
		when tg in('SPU') or mobile_no in(select mobile_no from tallykhata.tk_spu_aspu_data where pu_type in('SPU', 'Sticky SPU') and report_date='2022-03-11') then 'SPU'
		when tg ilike 'pu%' then 'PUAll'
		when tg ilike '3rau%'  then '3RAUAll'
		when tg ilike 'ltu%' then 'LTUAll' 
		when tg ilike 'z%' then 'ZAll'
		when tg ilike 'psu%' then 'PSU'
		when tg in('NN2-6', 'NN1', 'NB0') then 'NN'
		when tg ilike '%NT%' then 'NT' 
	end tg_shrunk
from cjm_segmentation.retained_users 
where report_date='2022-03-11';
*/

-- txn SMS cost statistics
do $$

declare 
	var_date date:='2021-12-01'::date; 
begin  
	raise notice 'New OP goes below:'; 

	drop table if exists data_vajapora.temp_a; 
	create table data_vajapora.temp_a as
	select id, translate(substring(message_body, length(message_body)-10, 12), '০১২৩৪৫৬৭৮৯', '0123456789') mobile_no, request_time, telco_identifier_id
	from public.t_scsms_message_archive_v2
	where
		1=1
		and channel='TALLYKHATA_TXN'
		and bank_name='SURECASH'
		and lower(message_body) not like '%verification code%'
		and message_body not like '%অনুগ্রহ করে%'
		and telco_identifier_id in(66, 64, 61, 62, 49, 67) 
		and upper(message_status) in('SUCCESS', '0'); 
	raise notice 'Txn SMSs extracted.';
	
	loop
		delete from data_vajapora.txn_sms_stats_3
		where report_date=var_date;
	
		drop table if exists data_vajapora.temp_b; 
		create table data_vajapora.temp_b as
		select 
			mobile_no, 
			case 
				when tg in('SPU') or mobile_no in(select mobile_no from tallykhata.tk_spu_aspu_data where pu_type in('SPU', 'Sticky SPU') and report_date=var_date) then 'SPU'
				when tg ilike 'pu%' then 'PUAll'
				when tg ilike '3rau%'  then '3RAUAll'
				when tg ilike 'ltu%' then 'LTUAll' 
				when tg ilike 'z%' then 'ZAll'
				when tg ilike 'psu%' then 'PSU'
				when tg in('NN2-6', 'NN1', 'NB0') then 'NN'
				when tg ilike '%NT%' then 'NT' 
			end tg_shrunk
		from cjm_segmentation.retained_users 
		where report_date=var_date; 
		
		insert into data_vajapora.txn_sms_stats_3
		select 
			var_date report_date, 
			sum(cost) txn_sms_cost, 
			sum(case when tg_shrunk='SPU' then cost else null end) txn_sms_cost_SPU, 
			sum(case when tg_shrunk='PUAll' then cost else null end) txn_sms_cost_PUAll, 
			sum(case when tg_shrunk='3RAUAll' then cost else null end) txn_sms_cost_3RAUAll, 
			sum(case when tg_shrunk='LTUAll' then cost else null end) txn_sms_cost_LTUAll, 
			sum(case when tg_shrunk='ZAll' then cost else null end) txn_sms_cost_ZAll, 
			sum(case when tg_shrunk='PSU' then cost else null end) txn_sms_cost_PSU, 
			sum(case when tg_shrunk='NN' then cost else null end) txn_sms_cost_NN, 
			sum(case when tg_shrunk='NT' then cost else null end) txn_sms_cost_NT, 
			sum(case when tg_shrunk is null then cost else null end) txn_sms_cost_rest 
		from 
			(select id, mobile_no, telco_identifier_id
			from data_vajapora.temp_a
			where date(request_time)=var_date
			) tbl1 
			
			inner join 
			
			(select telco_identifier_id, cost 
			from data_vajapora.telco_costs
			) tbl3 using(telco_identifier_id)
			
			left join 
		
			data_vajapora.temp_b tbl2 using(mobile_no); 
		
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date='2022-01-01'::date then exit; 
		end if; 
	end loop; 
end $$; 

select * 
from data_vajapora.txn_sms_stats_3
order by 1; 

select 
	left(report_date::text, 7) report_month, 
	sum(txn_sms_cost) txn_sms_cost,
	sum(txn_sms_cost_spu) txn_sms_cost_spu,
	sum(txn_sms_cost_puall) txn_sms_cost_puall,
	sum(txn_sms_cost_3rauall) txn_sms_cost_3rauall,
	sum(txn_sms_cost_ltuall) txn_sms_cost_ltuall,
	sum(txn_sms_cost_zall) txn_sms_cost_zall,
	sum(txn_sms_cost_psu) txn_sms_cost_psu,
	sum(txn_sms_cost_nn) txn_sms_cost_nn,
	sum(txn_sms_cost_nt) txn_sms_cost_nt,
	sum(txn_sms_cost_rest) txn_sms_cost_rest
from data_vajapora.txn_sms_stats_3
group by 1
order by 1; 
