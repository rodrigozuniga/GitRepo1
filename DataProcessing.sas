/*This code is used to process mobile app downloads*/

data mb.vdang_dtx_clickstream_T (compress=yes);
set mb.vdang_dtx_clickstream;
if _N_<1000;
iPhone=(indexw(lowcase(device_type),"iphone","|")>0); 
ipad  =(indexw(lowcase(device_type),"ipad")>0); 
AndroidPhone=(indexw(lowcase(device_type),"phone")>0);
AndroidTablet=(indexw(lowcase(device_type),"tablet")>0); 
WebBrowser=(sum(iphone,ipad,AndroidPhone,AndroidTablet)=0);
run;


proc sql;
connect to odbc(dsn=USG_SBG_WS user=yuanc356 pwd=Yuan@123);
create table mb.vdang_dtx_clickstream_IOS (compress=yes) as
select * from connection to odbc
(
select
HIT_TIMESTAMP,
POST_PROP17 as location,
POST_PROP24	as page,
post_evar65 as action,
post_prop13 as filter_value,
post_prop21 as edit_value,
post_evar59 as company_id,
post_evar01 as device_type,
post_evar62 as language_local,
'ios_native_app' as platform
from
USG_SBG.USG_SBG_ETL.TRANS_CLICKSTREAM_MOBILE_SBG_380
where
hit_timestamp between '11-01-2014' and current_date
and post_prop17 = 'banking'
);
quit;