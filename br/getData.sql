drop table if exists impression;
create external table if not exists impression
(
    pageid string,
    pageindex int,
    newsid int,
    userid string,
    time string,
    tag string,
    appversion int,
    metaversion string,
    devicetype string,
    deviceplatform string,
    imei string,
    imsi string,
    dpi string,
    resolution string,
    osversion string,
    osapi string,
    latitude string,
    longtitude string,
    network string,
    secret string,
    countryid string,
    updateversioncode int,
    newstype string,
    mediaid int,
    imagecount int,
    videolength int,
    categoryid int,
    hot string,
    googleadid string,
    gogleadstatus string,
    userip string,
    requestcategoryid int
)
partitioned by (d string, h string)
row format delimited fields terminated by '\t';

alter table impression add partition(d='${hiveconf:date}', h='${hiveconf:hour_current}') location 'wasb://niphdbr@nipspark.blob.core.windows.net/dailyCtr/NewsImpressionSummary-${hiveconf:date}/${hiveconf:hour_current}';


drop table if exists click;
create external table if not exists click
(
    pageid string,
    pageindex int,
    newsid int,
    userid string,
    time string,
    index int,
    appversion int,
    metaversion string,
    devicetype string,
    deviceplatform string,
    imei string,
    imsi string,
    dpi string,
    resolution string,
    osversion string,
    osapi string,
    latitude string,
    longtitude string,
    network string,
    secret string,
    countryid string,
    updateversioncode int,
    googleadid string,
    gogleadstatus string,
    userip string
)
partitioned by (d string, h string)
row format delimited fields terminated by '\t';

alter table click add partition(d='${hiveconf:date}', h='${hiveconf:hour_left1}') location 'wasb://niphdbr@nipspark.blob.core.windows.net/dailyCtr/NewsClickSummary-${hiveconf:date}/${hiveconf:hour_left1}';
alter table click add partition(d='${hiveconf:date}', h='${hiveconf:hour_current}') location 'wasb://niphdbr@nipspark.blob.core.windows.net/dailyCtr/NewsClickSummary-${hiveconf:date}/${hiveconf:hour_current}';
alter table click add partition(d='${hiveconf:date}', h='${hiveconf:hour_right1}') location 'wasb://niphdbr@nipspark.blob.core.windows.net/dailyCtr/NewsClickSummary-${hiveconf:date}/${hiveconf:hour_right1}';
alter table click add partition(d='${hiveconf:date}', h='${hiveconf:hour_right2}') location 'wasb://niphdbr@nipspark.blob.core.windows.net/dailyCtr/NewsClickSummary-${hiveconf:date}/${hiveconf:hour_right2}';
alter table click add partition(d='${hiveconf:date}', h='${hiveconf:hour_right3}') location 'wasb://niphdbr@nipspark.blob.core.windows.net/dailyCtr/NewsClickSummary-${hiveconf:date}/${hiveconf:hour_right3}';




drop table if exists dianzan;
create external table if not exists dianzan
(
    userid string,
    newsid string,
    vote_up int,
    vote_down int
)
partitioned by (d string, h string)
row format delimited fields terminated by '\t';

alter table dianzan add partition(d='${hiveconf:date}', h='${hiveconf:hour_left8}') location 'wasb://niphdbr@nipspark.blob.core.windows.net/dashboard_offline_dt/${hiveconf:date}/${hiveconf:hour_left8}00';
alter table dianzan add partition(d='${hiveconf:date}', h='${hiveconf:hour_left7}') location 'wasb://niphdbr@nipspark.blob.core.windows.net/dashboard_offline_dt/${hiveconf:date}/${hiveconf:hour_left7}00';
alter table dianzan add partition(d='${hiveconf:date}', h='${hiveconf:hour_left6}') location 'wasb://niphdbr@nipspark.blob.core.windows.net/dashboard_offline_dt/${hiveconf:date}/${hiveconf:hour_left6}00';
alter table dianzan add partition(d='${hiveconf:date}', h='${hiveconf:hour_left5}') location 'wasb://niphdbr@nipspark.blob.core.windows.net/dashboard_offline_dt/${hiveconf:date}/${hiveconf:hour_left5}00';
alter table dianzan add partition(d='${hiveconf:date}', h='${hiveconf:hour_left4}') location 'wasb://niphdbr@nipspark.blob.core.windows.net/dashboard_offline_dt/${hiveconf:date}/${hiveconf:hour_left4}00';


drop table if exists click_show_join;
create table click_show_join
(
    pageindex int,
    tag string,
    newstype string,
    mediaid int,
    categoryid int,
    requestcategoryid int,
    index int,
    vote_up int,
    voite_down int
)
row format delimited fields terminated by '\t';

INSERT OVERWRITE TABLE click_show_join
select
    impression.pageindex,
    impression.tag,
    impression.newstype,
    impression.mediaid,
    impression.categoryid,
    impression.requestcategoryid,
    click.index,
    dianzan.vote_up,
    dianzan.vote_down
FROM
impression
    LEFT JOIN
click
    ON
(
    impression.userid = click.userid
    and impression.newsid = click.newsid
    and impression.pageid = click.pageid
    and impression.pageindex = click.pageindex
)
    LEFT JOIN
dianzan
    ON
(
    impression.userid = dianzan.userid
    and impression.newsid = dianzan.newsid
);

INSERT OVERWRITE DIRECTORY 'wasb://niphdbr@nipspark.blob.core.windows.net/user/zhangrn/click_show_join/${hiveconf:date}/${hiveconf:hour_current}00' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' SELECT * from click_show_join;
