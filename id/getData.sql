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
partitioned by (y string, m string, d string, h string)
row format delimited fields terminated by '\t';

alter table impression add partition(y='${hiveconf:year}', m='${hiveconf:month}', d='${hiveconf:day}', h='${hiveconf:hour_left4}') location 'wasb://id-newsimpression@nipspark.blob.core.windows.net/${hiveconf:year}/${hiveconf:month}/${hiveconf:day}/${hiveconf:hour_left4}/';


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
partitioned by (y string, m string, d string, h string)
row format delimited fields terminated by '\t';

alter table click add partition(y='${hiveconf:year}', m='${hiveconf:month}', d='${hiveconf:day}', h='${hiveconf:hour_left4}') location 'wasb://id-newsclick@nipspark.blob.core.windows.net/${hiveconf:year}/${hiveconf:month}/${hiveconf:day}/${hiveconf:hour_left4}/';
alter table click add partition(y='${hiveconf:year}', m='${hiveconf:month}', d='${hiveconf:day}', h='${hiveconf:hour_left3}') location 'wasb://id-newsclick@nipspark.blob.core.windows.net/${hiveconf:year}/${hiveconf:month}/${hiveconf:day}/${hiveconf:hour_left3}/';
alter table click add partition(y='${hiveconf:year}', m='${hiveconf:month}', d='${hiveconf:day}', h='${hiveconf:hour_left2}') location 'wasb://id-newsclick@nipspark.blob.core.windows.net/${hiveconf:year}/${hiveconf:month}/${hiveconf:day}/${hiveconf:hour_left2}/';
alter table click add partition(y='${hiveconf:year}', m='${hiveconf:month}', d='${hiveconf:day}', h='${hiveconf:hour_left1}') location 'wasb://id-newsclick@nipspark.blob.core.windows.net/${hiveconf:year}/${hiveconf:month}/${hiveconf:day}/${hiveconf:hour_left1}/';


drop table if exists click_show_join;
create temporary table if not exists click_show_join as
select
    impression.*,
    IF(click.newsid is NULL, 0, 1) as clicked,
    click.index
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
);

INSERT OVERWRITE DIRECTORY 'wasb://niphdid@nipspark.blob.core.windows.net/user/zhangrn/click_show_join/${hiveconf:date}/${hiveconf:hour_left4}00' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' SELECT * from click_show_join;
