-- Code to get NPS for QBO mobile users
-- SBG_SANDBOX.NPS_SURVEY_RESPONSES;
-- firm and company_id do nvl function answer3 and answer4

--create NPS table
drop table if exists SBG_SANDBOX.rz_NPS_SURVEY_RESPONSES;
create table SBG_SANDBOX.rz_NPS_SURVEY_RESPONSES AS
select 
  cast(trim(nvl(answer_3,answer_4)) as varchar(20)) as company_id
, cast(trim(answer_2) as varchar(25)) as user_id
, date(createdTs) as survey_date
, round(cast(answer_0 as numeric(3,0)), 0) as np_raw 
, case 
   when cast(answer_0 as numeric) <=  6 then 'Detractor'
   when cast(answer_0 as numeric) >=7 and cast(answer_0 as numeric) <=8 then 'Passive'   
   when cast(answer_0 as numeric) >=  9 then 'Promoter'
  end as np_segment
, case 
   when cast(answer_0 as numeric) <=  6 then -1
   when cast(answer_0 as numeric) >=7 and cast(answer_0 as numeric) <=8 then 0  
   when cast(answer_0 as numeric) >=  9 then 1
  end as np_score
, answer_1 as np_voice
from thrive_dwh.cto_prod_survey
where surveyid = 'crq2pcnh' --and traffictype = 'live' and answer_3 <> '' and answer_3 <> 'company_id' and date(survey_date) > current_date - 60
group by 1,2,3,4,5,6,7;

select count(*) from SBG_SANDBOX.rz_NPS_SURVEY_RESPONSES; 

GRANT SELECT ON SBG_SANDBOX.rz_NPS_SURVEY_RESPONSES TO SBG_ETL, SBG_ANALYST, SBG_ADMIN, SBG_RPT
;

--Add beggining of week date:
drop table if exists SBG_SANDBOX.rz_NPS_SURVEY_RESPONSES2;
create table SBG_SANDBOX.rz_NPS_SURVEY_RESPONSES2 AS
select company_id
,user_id
,survey_date
,survey_date - DATE_PART('DOW',survey_date) +1 as wk_survey
,last_day(survey_date-15) as ym_survey_date
,np_raw
,np_segment
,np_score
,np_voice
,date(survey_date + 1 - extract('day' from survey_date)) as first_day_month_survey
,date(last_day(survey_date)) as last_day_month_survey
,date(survey_date - extract('day' from survey_date)) as last_day_prev_month_survey
,date(date(survey_date - extract('day' from survey_date))+1-extract('day' from date(survey_date - extract('day' from survey_date)))) as first_day_prev_month_survey
from sbg_sandbox.rz_NPS_SURVEY_RESPONSES a
where company_id !='fwewef'
; 

GRANT SELECT ON SBG_SANDBOX.rz_NPS_SURVEY_RESPONSES2 TO SBG_ETL, SBG_ANALYST, SBG_ADMIN, SBG_RPT
;

--Add last 4 wks or last 3 mnths activity levels
drop table if exists SBG_SANDBOX.rz_NPS_SURVEY_RESPONSES3;
create table SBG_SANDBOX.rz_NPS_SURVEY_RESPONSES3 AS
select a.* 
 ,coalesce(b.non_mbl_active_last_4wks_flag,0) as non_mbl_active_last_4wks_flag
 ,coalesce(b.mbl_active_last_4wks_flag,0) as mbl_active_last_4wks_flag
 ,coalesce(b.mbl_active_each_last_4wks_flag,0) as mbl_active_each_last_4wks_flag
 ,coalesce(c.non_mbl_active_each_last_three_months_flag,0) as non_mbl_active_each_last_three_months_flag
 ,coalesce(c.mbl_active_each_last_three_months_flag,0) as mbl_active_each_last_three_months_flag 
from SBG_SANDBOX.rz_NPS_SURVEY_RESPONSES2 a 
       left join sbg_sandbox.rz_mbl_4wk b on a.company_id=b.company_id and a.wk_survey=b.wk
       left join sbg_sandbox.rz_mbl_3m c on a.company_id=c.company_id and a.ym_survey_date=c.ym
;

select * from SBG_SANDBOX.rz_NPS_SURVEY_RESPONSES3 limit 50;

select ym_survey_date
,sum(non_mbl_active_last_4wks_flag) as non_mbl_active_last_4wks_flag 
,sum(mbl_active_last_4wks_flag) as mbl_active_last_4wks
,sum(mbl_active_each_last_4wks_flag) as mbl_active_each_last_4wks
,sum(non_mbl_active_each_last_three_months_flag) as non_mbl_active_each_last_three_months_flag
,sum(mbl_active_each_last_three_months_flag) as mbl_active_each_last_three_months_flag 
from SBG_SANDBOX.rz_NPS_SURVEY_RESPONSES3 
group by 1
order by 1;

--create unique sequence to differentiate individual records in Tableau
drop sequence sbg_sandbox.my_seq;
create sequence sbg_sandbox.my_seq START 1;

ALTER TABLE SBG_SANDBOX.rz_NPS_SURVEY_RESPONSES3 ADD COLUMN SID INT;
update SBG_SANDBOX.rz_NPS_SURVEY_RESPONSES3 set SID=NEXTVAL('sbg_sandbox.my_seq');

select count(*), count(distinct SID) from SBG_SANDBOX.rz_NPS_SURVEY_RESPONSES3; 

drop table if exists SBG_SANDBOX.rz_NPS_SURVEY_RESPONSES4;
create table SBG_SANDBOX.rz_NPS_SURVEY_RESPONSES4 AS
select a.*
,b.company_id as cid
,b.main_device
,b.total_logins
,b.non_mobile_login
,b.region
,b.qbo_channel_aggr_name
,b.qbo_offer
,b.trial_flag
,b.ym
,coalesce(b.mobile_flag,0) as mobile_flag
from SBG_SANDBOX.rz_NPS_SURVEY_RESPONSES3 a left join sbg_sandbox.rz_mbl_month_summ b 
        on a.company_id=b.company_id  
        and a.ym_survey_date=b.ym
;

GRANT SELECT ON SBG_SANDBOX.rz_NPS_SURVEY_RESPONSES4 TO SBG_ETL, SBG_ANALYST, SBG_ADMIN, SBG_RPT;

select count(*) from SBG_SANDBOX.rz_NPS_SURVEY_RESPONSES4;
















--Add monthly range of dates
drop table if exists SBG_SANDBOX.rz_NPS_SURVEY_RESPONSES2;
create table SBG_SANDBOX.rz_NPS_SURVEY_RESPONSES2 AS
select company_id
,user_id
,survey_date
,np_raw
,np_segment
,np_score
,np_voice
,date(survey_date + 1 - extract('day' from survey_date)) as first_day_month_survey
,date(last_day(survey_date)) as last_day_month_survey
,date(survey_date - extract('day' from survey_date)) as last_day_prev_month_survey
,date(date(survey_date - extract('day' from survey_date))+1-extract('day' from date(survey_date - extract('day' from survey_date)))) as first_day_prev_month_survey
from sbg_sandbox.rz_NPS_SURVEY_RESPONSES 
; 

GRANT SELECT ON SBG_SANDBOX.rz_NPS_SURVEY_RESPONSES2 TO SBG_ETL, SBG_ANALYST, SBG_ADMIN, SBG_RPT
;











select * from SBG_SANDBOX.rz_NPS_SURVEY_RESPONSES2 limit 100;

--Select only those that were mobile the month previous to the survey
drop table if exists SBG_SANDBOX.rz_NPS_SURVEY_RESPONSES3;
create table SBG_SANDBOX.rz_NPS_SURVEY_RESPONSES3 AS
select a.*
,b.company_id as cid
,b.main_device
,b.total_logins
,b.non_mobile_login
,b.region
,b.qbo_channel_aggr_name
,b.qbo_offer
,b.trial_flag
,b.ym
,coalesce(b.mobile_flag,0) as mobile_flag
from SBG_SANDBOX.rz_NPS_SURVEY_RESPONSES2 a left join sbg_sandbox.rz_mbl_month_summ b 
        on a.company_id=cast(b.company_id as varchar) 
        and a.last_day_prev_month_survey=b.ym
;

GRANT SELECT ON SBG_SANDBOX.rz_NPS_SURVEY_RESPONSES3 TO SBG_ETL, SBG_ANALYST, SBG_ADMIN, SBG_RPT
;

select mobile_flag,count(*) from SBG_SANDBOX.rz_NPS_SURVEY_RESPONSES3 group by 1;



--NPS Mobile only by year and month
select 
last_day_prev_month_survey
, mobile_flag
, count(*) as company_count
, round(sum(np_score) / count(*)*100,1.0) as nps 
from
SBG_SANDBOX.rz_NPS_SURVEY_RESPONSES3 
where main_device !='windows'
and qbo_offer !='QBOE'
and region='United States'
group by 1,2      
order by 1,2;

--Group by mobile/no mobile 
drop table if exists SBG_SANDBOX.rz_NPS_SURVEY_RESPONSES4;
create table SBG_SANDBOX.rz_NPS_SURVEY_RESPONSES4 AS
select last_day_month_survey
, mobile_flag
, qbo_offer
, qbo_channel_aggr_name
, trial_flag
, region
, main_device
, np_segment
, count(*)
from SBG_SANDBOX.rz_NPS_SURVEY_RESPONSES3 
group by 1,2,3,4,5,6,7,8
order by 2,1,3,4,5,6,7,8
;
     
GRANT SELECT ON SBG_SANDBOX.rz_NPS_SURVEY_RESPONSES4 TO SBG_ETL, SBG_ANALYST, SBG_ADMIN, SBG_RPT
;








