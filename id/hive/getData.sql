drop table if exists click_show_zan_join;
create external table if not exists click_show_zan_join
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
    requestcategoryid int,
    clicked int,
    click_index int
)
partitioned by (d string)
row format delimited fields terminated by '\t';

alter table click_show_zan_join add partition(d='${hiveconf:date}') location 'wasb://niphdid@nipspark.blob.core.windows.net/user/zhangrn/click_show_join/${hiveconf:date}/';

INSERT OVERWRITE LOCAL DIRECTORY '/home/renning/tiny_work/click_show_zan_join/id/hive/data/${hiveconf:date}/' row format delimited fields terminated by '\t'
SELECT
    tag,
    newstype,
    mediaid,
    categoryid,
    requestcategoryid,
    click_index,
    COUNT(*),
    SUM(clicked)
FROM
    click_show_zan_join
GROUP BY
    tag,
    newstype,
    mediaid,
    categoryid,
    requestcategoryid,
    click_index;
