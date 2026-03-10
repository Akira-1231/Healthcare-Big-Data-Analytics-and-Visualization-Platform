
  
    

  create  table "ecg_dw"."dev"."dim_time__dbt_tmp"
  
  
    as
  
  (
    

SELECT TO_CHAR(time_value, 'HH24MISS')::INT AS time_key,
       time_value AS time_actual,
       EXTRACT(EPOCH FROM time_value) AS epoch,
       EXTRACT(HOUR FROM time_value) AS hour_actual,
       TO_CHAR(time_value, 'HH24') AS hour_24h,
       TO_CHAR(time_value, 'HH12') AS hour_12h,
       TO_CHAR(time_value, 'AM') AS am_pm,
       EXTRACT(MINUTE FROM time_value) AS minute_actual,
       EXTRACT(SECOND FROM time_value) AS second_actual,
       TO_CHAR(time_value, 'MI') AS minute_name,
       TO_CHAR(time_value, 'SS') AS second_name,
       CASE
           WHEN EXTRACT(HOUR FROM time_value) < 12 THEN 'Morning'
           WHEN EXTRACT(HOUR FROM time_value) < 17 THEN 'Afternoon'
           WHEN EXTRACT(HOUR FROM time_value) < 20 THEN 'Evening'
           ELSE 'Night'
           END AS time_of_day
FROM (SELECT '00:00:00'::TIME + SEQUENCE.SECOND * INTERVAL '1 second' AS time_value
      FROM GENERATE_SERIES(0, 86399) AS SEQUENCE (SECOND)
      GROUP BY SEQUENCE.SECOND) TQ
ORDER BY 1
  );
  