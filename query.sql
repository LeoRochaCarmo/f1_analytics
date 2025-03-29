-- Biggest winners by Grand Prix

SELECT
    forename || ' ' || surname as drivers_name,
    t2.name as race_name,
    COUNT(*) as total_wins


FROM results AS t1

LEFT JOIN races AS t2
ON t1.raceId = t2.raceId

LEFT JOIN drivers as t3
ON t1.driverId = t3.driverId

WHERE position = 1

GROUP BY drivers_name, race_name

ORDER BY total_wins DESC

-- Drivers with most DNFs

SELECT
    forename || ' ' || surname as drivers_name,
    COUNT(*) num_of_dnfs

FROM results AS t1

LEFT JOIN status AS t2
ON t1.statusId = t2.statusId

LEFT JOIN drivers as t3
ON t1.driverId = t3.driverId

WHERE status IS NOT 'Finished'

GROUP BY drivers_name

ORDER BY num_of_dnfs DESC

-- Highest finish rates from drivers with more than 100 races

WITH num_of_races AS (
    SELECT 
        forename || ' ' || surname as drivers_name,
        COUNT(*) num_of_races
    FROM results AS t1

    LEFT JOIN drivers AS t2
    ON t1.driverId = t2.driverId

    GROUP BY drivers_name 

), 

num_of_finishes AS (
    SELECT
        forename || ' ' || surname as drivers_name,
        COUNT(*) num_of_finishes

    FROM results AS t1

    LEFT JOIN status AS t2
    ON t1.statusId = t2.statusId

    LEFT JOIN drivers as t3
    ON t1.driverId = t3.driverId

    WHERE status = 'Finished'

    GROUP BY drivers_name
)

SELECT
    t1.drivers_name,
    num_of_races,
    COALESCE(num_of_finishes, 0) AS num_of_finishes,
    ROUND(COALESCE((1.0 * num_of_finishes / num_of_races) * 100.0, 0), 2) AS finish_rate
   
FROM num_of_races AS t1

LEFT JOIN num_of_finishes AS t2
ON t1.drivers_name = t2.drivers_name

WHERE num_of_races > 100

ORDER BY finish_rate DESC








