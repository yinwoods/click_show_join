drop table if exists id_impression;
create external table if not exists id_impression
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

alter table id_impression add partition(y='${hiveconf:year}', m='${hiveconf:month}', d='${hiveconf:day}', h='${hiveconf:hour_left4}') location 'wasb://id-newsimpression@nipspark.blob.core.windows.net/${hiveconf:year}/${hiveconf:month}/${hiveconf:day}/${hiveconf:hour_left4}/';


drop table if exists id_click;
create external table if not exists id_click
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

alter table id_click add partition(y='${hiveconf:year}', m='${hiveconf:month}', d='${hiveconf:day}', h='${hiveconf:hour_left5}') location 'wasb://id-newsclick@nipspark.blob.core.windows.net/${hiveconf:year}/${hiveconf:month}/${hiveconf:day}/${hiveconf:hour_left5}/';
alter table id_click add partition(y='${hiveconf:year}', m='${hiveconf:month}', d='${hiveconf:day}', h='${hiveconf:hour_left4}') location 'wasb://id-newsclick@nipspark.blob.core.windows.net/${hiveconf:year}/${hiveconf:month}/${hiveconf:day}/${hiveconf:hour_left4}/';
alter table id_click add partition(y='${hiveconf:year}', m='${hiveconf:month}', d='${hiveconf:day}', h='${hiveconf:hour_left3}') location 'wasb://id-newsclick@nipspark.blob.core.windows.net/${hiveconf:year}/${hiveconf:month}/${hiveconf:day}/${hiveconf:hour_left3}/';
alter table id_click add partition(y='${hiveconf:year}', m='${hiveconf:month}', d='${hiveconf:day}', h='${hiveconf:hour_left2}') location 'wasb://id-newsclick@nipspark.blob.core.windows.net/${hiveconf:year}/${hiveconf:month}/${hiveconf:day}/${hiveconf:hour_left2}/';
alter table id_click add partition(y='${hiveconf:year}', m='${hiveconf:month}', d='${hiveconf:day}', h='${hiveconf:hour_left1}') location 'wasb://id-newsclick@nipspark.blob.core.windows.net/${hiveconf:year}/${hiveconf:month}/${hiveconf:day}/${hiveconf:hour_left1}/';


drop table if exists id_click_show_join;
create temporary table if not exists id_click_show_join as
select
    new_id_impression.*,
    IF(new_id_click.newsid is NULL, 0, 1) as id_clicked,
    new_id_click.index
FROM
(select distinct pageid, pageindex, newsid, userid, time, tag, appversion, metaversion, devicetype, deviceplatform, imei, imsi, dpi, resolution, osversion, osapi, latitude, longtitude, network, secret, countryid, updateversioncode, newstype, mediaid, imagecount, videolength, categoryid, hot, googleadid, googleadstatus, userip, requestcategoryid from id_impression) new_id_impression
    LEFT JOIN
(select distinct newsid, index, userid, pageid, pageindex from id_click) new_id_click
    ON
(
    new_id_impression.userid = new_id_click.userid
    and new_id_impression.newsid = new_id_click.newsid
    and new_id_impression.pageid = new_id_click.pageid
    and new_id_impression.pageindex = new_id_click.pageindex
);

INSERT OVERWRITE DIRECTORY 'wasb://niphdid@nipspark.blob.core.windows.net/user/zhangrn/click_show_join/${hiveconf:date}/${hiveconf:hour_left4}00' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' SELECT * from id_click_show_join;
