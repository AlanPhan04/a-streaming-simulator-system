-- SELECT * FROM unifieddata;

-- AIR INFO
SELECT
  air_station,
  AVG(AIR_TEMPERATURE) AS avg_temp,
  AVG(AIR_MOISTURE) AS avg_moisture,
  AVG(AIR_LIGHT) AS avg_light,
  AVG(AIR_RAINFALL) AS avg_rainfall,
  AVG("air_windDirection") AS avg_winddirection,
  AVG("air_pm2dot5") AS "avg_PM2.5",
  AVG(AIR_PM10) AS avg_pm10,
  AVG("air_CO") AS avg_co,
  AVG("air_NOx") AS avg_nox,
  AVG("air_SO2") AS avg_so2
FROM unifieddata
GROUP BY air_station
ORDER BY air_station;

-- WATER INFO
SELECT
  water_station,
  AVG(WATER_TEMPERATURE) AS avg_temp,
  AVG(WATER_SALINITY) AS avg_moisture,
  AVG("water_pH") AS avg_ph
FROM unifieddata
GROUP BY water_station
ORDER BY water_station;

-- EARTH INFO
SELECT
  earth_station,
  AVG(EARTH_TEMPERATURE) AS avg_temp,
  AVG(EARTH_MOISTURE) AS avg_moisture,
  AVG("earth_pH") AS avg_ph
FROM unifieddata
GROUP BY earth_station
ORDER BY earth_station;
