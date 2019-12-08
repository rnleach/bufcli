SELECT 
    valid_time,
    hdw,
    blow_up_dt,
    blow_up_meters,
    dcape 
FROM cli
WHERE site = ?1 and model = ?2
