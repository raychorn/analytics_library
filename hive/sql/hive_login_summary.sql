-- 0. database #TODO: Temporary
USE bouygues;

-- 1. drop login summary
DROP TABLE login_summary;

-- 2. create login summary
CREATE TABLE login_summary(
uuid	 STRING,
device_type	 STRING,
first_login	 STRING,
last_login	 STRING,
manufacturer	 STRING,
device_model	 STRING,
last_device_date	STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

--3. populate table data with login information
INSERT OVERWRITE TABLE login_summary
SELECT uuid, device_type, first_login_date, last_login_date, manufacturer, device_model, last_device_date
FROM
(select uuid, "WiFiDevice" AS device_type, MIN(to_date(c_58)) AS first_login_date, MAX(to_date(c_59)) AS last_login_date, c_193 AS manufacturer, c_194 AS device_model, MAX(to_date(server_date)) AS last_device_date
FROM logevents
WHERE en = "WiFiDeviceInsertion"
GROUP BY uuid, c_193, c_194
UNION ALL
select uuid, "3GDevice" AS device_type, MIN(to_date(c_64)) AS first_login_date, MAX(to_date(c_65)) AS last_login_date, c_36 AS manufacturer, c_37 AS device_model, MAX(to_date(server_date)) AS last_device_date
FROM logevents
WHERE en = "3GDeviceInsertion"
GROUP BY uuid, c_36, c_37
UNION ALL
select uuid, "4GDevice" AS device_type, MIN(to_date(c_26)) AS first_login_date, MAX(to_date(c_27)) AS last_login_date, c_195 AS manufacturer, c_196 AS device_model, MAX(to_date(server_date)) AS last_device_date
FROM logevents
WHERE en = "4GDeviceInsertion"
GROUP BY uuid, c_195, c_196
UNION ALL
select uuid, "EthernetDevice" AS device_type, MIN(to_date(c_176)) AS first_login_date, MAX(to_date(c_177)) AS last_login_date, c_36 AS manufacturer, c_37 AS device_model, MAX(to_date(server_date)) AS last_device_date
FROM logevents
WHERE en = "EthernetDeviceInsertion"
GROUP BY uuid, c_36, c_37) a;