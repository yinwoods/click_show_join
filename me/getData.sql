drop table if exists me_impression;
create external table if not exists me_impression
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
    googleadstatus string,
    userip string,
    requestcategoryid int
)
partitioned by (y string, m string, d string, h string)
row format delimited fields terminated by '\t';

alter table me_impression add partition(y='${hiveconf:year}', m='${hiveconf:month}', d='${hiveconf:day}', h='${hiveconf:hour_left4}') location 'wasb://me-newsimpression@nipspark.blob.core.windows.net/${hiveconf:year}/${hiveconf:month}/${hiveconf:day}/${hiveconf:hour_left4}/';


drop table if exists me_click;
create external table if not exists me_click
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

alter table me_click add partition(y='${hiveconf:year}', m='${hiveconf:month}', d='${hiveconf:day}', h='${hiveconf:hour_left5}') location 'wasb://me-newsclick@nipspark.blob.core.windows.net/${hiveconf:year}/${hiveconf:month}/${hiveconf:day}/${hiveconf:hour_left5}/';
alter table me_click add partition(y='${hiveconf:year}', m='${hiveconf:month}', d='${hiveconf:day}', h='${hiveconf:hour_left4}') location 'wasb://me-newsclick@nipspark.blob.core.windows.net/${hiveconf:year}/${hiveconf:month}/${hiveconf:day}/${hiveconf:hour_left4}/';
alter table me_click add partition(y='${hiveconf:year}', m='${hiveconf:month}', d='${hiveconf:day}', h='${hiveconf:hour_left3}') location 'wasb://me-newsclick@nipspark.blob.core.windows.net/${hiveconf:year}/${hiveconf:month}/${hiveconf:day}/${hiveconf:hour_left3}/';
alter table me_click add partition(y='${hiveconf:year}', m='${hiveconf:month}', d='${hiveconf:day}', h='${hiveconf:hour_left2}') location 'wasb://me-newsclick@nipspark.blob.core.windows.net/${hiveconf:year}/${hiveconf:month}/${hiveconf:day}/${hiveconf:hour_left2}/';
alter table me_click add partition(y='${hiveconf:year}', m='${hiveconf:month}', d='${hiveconf:day}', h='${hiveconf:hour_left1}') location 'wasb://me-newsclick@nipspark.blob.core.windows.net/${hiveconf:year}/${hiveconf:month}/${hiveconf:day}/${hiveconf:hour_left1}/';


drop table if exists me_click_show_join;
create temporary table if not exists me_click_show_join as
select
    new_me_impression.*,
    IF(new_me_click.newsid is NULL, 0, 1) as me_clicked,
    new_me_click.index
FROM
(select distinct pageid, pageindex, newsid, userid, time, tag, appversion, metaversion, devicetype, deviceplatform, imei, imsi, dpi, resolution, osversion, osapi, latitude, longtitude, network, secret, countryid, updateversioncode, newstype, mediaid, imagecount, videolength, categoryid, hot, googleadid, googleadstatus, userip, requestcategoryid from me_impression) new_me_impression
    LEFT JOIN
(select distinct newsid, index, userid, pageid, pageindex from me_click) new_me_click
    ON
(
    new_me_impression.userid = new_me_click.userid
    and new_me_impression.newsid = new_me_click.newsid
    and new_me_impression.pageid = new_me_click.pageid
    and new_me_impression.pageindex = new_me_click.pageindex
);

INSERT OVERWRITE DIRECTORY 'wasb://niphdme@nipspark.blob.core.windows.net/user/zhangrn/click_show_join/${hiveconf:date}/${hiveconf:hour_left4}00' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' SELECT * from me_click_show_join;
