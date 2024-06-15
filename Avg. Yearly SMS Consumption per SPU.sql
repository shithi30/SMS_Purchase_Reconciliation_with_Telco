-- Bhaiya can I get SMS per SPU per year?

-- tagada/txn SMS of year
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select id, translate(substring(message_body, length(message_body)-10, 12), '০১২৩৪৫৬৭৮৯', '0123456789') mobile_no
from public.t_scsms_message_archive_v2 as s
where
	upper(s.channel) in('TALLYKHATA_TXN') 
	and upper(trim(s.bank_name)) = 'SURECASH'
	and lower(s.message_body) not like '%verification code%'
	and s.telco_identifier_id in(66, 64, 61, 62, 49, 67) 
	and upper(s.message_status) in ('SUCCESS', '0')
	and left(s.request_time::text, 4)='2021'; 

-- SPUs of year
drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select distinct mobile_no
from tallykhata.tk_spu_aspu_data 
where 
	pu_type in('SPU', 'Sticky SPU')
	and left(report_date::text, 4)='2021'; 

select count(id) sms, count(distinct mobile_no) spu, count(id)*1.00/count(distinct mobile_no) avg_sms_spu
from 
	data_vajapora.help_a tbl1 
	inner join 
	data_vajapora.help_b tbl2 using(mobile_no);

