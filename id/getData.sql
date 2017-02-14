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

alter table impression add partition(d='${hiveconf:date}', h='${hiveconf:hour_left4}') location 'wasb://niphdid@nipspark.blob.core.windows.net/dailyCtr/NewsImpressionSummary-${hiveconf:date}/${hiveconf:hour_left4}';


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

alter table click add partition(d='${hiveconf:date}', h='${hiveconf:hour_left5}') location 'wasb://niphdid@nipspark.blob.core.windows.net/dailyCtr/NewsClickSummary-${hiveconf:date}/${hiveconf:hour_left5}';
alter table click add partition(d='${hiveconf:date}', h='${hiveconf:hour_left4}') location 'wasb://niphdid@nipspark.blob.core.windows.net/dailyCtr/NewsClickSummary-${hiveconf:date}/${hiveconf:hour_left4}';
alter table click add partition(d='${hiveconf:date}', h='${hiveconf:hour_left3}') location 'wasb://niphdid@nipspark.blob.core.windows.net/dailyCtr/NewsClickSummary-${hiveconf:date}/${hiveconf:hour_left3}';
alter table click add partition(d='${hiveconf:date}', h='${hiveconf:hour_left2}') location 'wasb://niphdid@nipspark.blob.core.windows.net/dailyCtr/NewsClickSummary-${hiveconf:date}/${hiveconf:hour_left2}';
alter table click add partition(d='${hiveconf:date}', h='${hiveconf:hour_left1}') location 'wasb://niphdid@nipspark.blob.core.windows.net/dailyCtr/NewsClickSummary-${hiveconf:date}/${hiveconf:hour_left1}';


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

alter table dianzan add partition(d='${hiveconf:date}', h='${hiveconf:hour_left12}') location 'wasb://niphdid@nipspark.blob.core.windows.net/dashboard_offline_dt/${hiveconf:date}/${hiveconf:hour_left12}00';
alter table dianzan add partition(d='${hiveconf:date}', h='${hiveconf:hour_left11}') location 'wasb://niphdid@nipspark.blob.core.windows.net/dashboard_offline_dt/${hiveconf:date}/${hiveconf:hour_left11}00';
alter table dianzan add partition(d='${hiveconf:date}', h='${hiveconf:hour_left10}') location 'wasb://niphdid@nipspark.blob.core.windows.net/dashboard_offline_dt/${hiveconf:date}/${hiveconf:hour_left10}00';
alter table dianzan add partition(d='${hiveconf:date}', h='${hiveconf:hour_left9}') location 'wasb://niphdid@nipspark.blob.core.windows.net/dashboard_offline_dt/${hiveconf:date}/${hiveconf:hour_left9}00';
alter table dianzan add partition(d='${hiveconf:date}', h='${hiveconf:hour_left8}') location 'wasb://niphdid@nipspark.blob.core.windows.net/dashboard_offline_dt/${hiveconf:date}/${hiveconf:hour_left8}00';


drop table if exists click_show_join;
create temporary table if not exists click_show_join as
select
    impression.pageindex,
    impression.tag,
    impression.newstype,
    impression.mediaid,
    impression.categoryid,
    impression.requestcategoryid,
    IF(click.newsid is NULL, 0, 1) as clicked,
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

INSERT OVERWRITE DIRECTORY 'wasb://niphdid@nipspark.blob.core.windows.net/user/zhangrn/click_show_join/${hiveconf:date}/${hiveconf:hour_left4}' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' SELECT * from click_show_join;
