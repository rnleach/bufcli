SELECT 
    valid_time,
    hdw,
    el_blow_up_dt,
    el_blow_up_meters,
    dcape 
FROM cli
WHERE station_num = ?1 and model = ?2
