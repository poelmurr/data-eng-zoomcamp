# Module 1 Homework - SQL Queries

-- QUESTION 3. For the trips in November 2025 (lpep_pickup_datetime between '2025-11-01' and '2025-12-01',
-- exclusive of the upper bound), how many trips had a trip_distance of less than or equal to 1 mile?
SELECT
	COUNT(1)
FROM
	green_taxi_2025_11
WHERE
	trip_distance <= 1 AND
	lpep_pickup_datetime >= '2025-11-01' AND
	lpep_pickup_datetime <  '2025-12-01';

-- ANSWER: 8007

-- QUESTION 4. Which was the pick up day with the longest trip distance? Only consider trips with trip_distance
-- less than 100 miles (to exclude data errors).
SELECT
	CAST(lpep_pickup_datetime as DATE) as "day",
	MAX(trip_distance) as "total_trip_distance"
FROM
	green_taxi_2025_11
WHERE
	trip_distance < 100
GROUP BY 1
ORDER BY 2 DESC;

-- ANSWER: 2025-11-14 with 88.03 miles

-- QUESTION 5. Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025?
SELECT
	z."Zone" as "zone",
	SUM(g.total_amount) as "total_amount",
	CAST(g.lpep_pickup_datetime as DATE) as "day"
FROM
	green_taxi_2025_11 g
JOIN
	zones z ON g."PULocationID" = z."LocationID"
WHERE
	CAST(g.lpep_pickup_datetime as DATE) = '2025-11-18'
GROUP BY 1,3
ORDER BY 2 DESC;

-- ANSWER: East Harlem North with $9,281.92

-- QUESTION 6. For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop
-- off zone that had the largest tip?
SELECT
	zdo."Zone" as "drop_off_zone",
	MAX(g.tip_amount) as "tip_amount"
FROM
	green_taxi_2025_11 g
JOIN
	zones zpu ON g."PULocationID" = zpu."LocationID"
JOIN
	zones zdo ON g."DOLocationID" = zdo."LocationID"
WHERE
	CAST(g.lpep_pickup_datetime as DATE) >= '2025-11-01' AND
	CAST(g.lpep_pickup_datetime as DATE) <= '2025-11-30' AND
	g."PULocationID" = 74
GROUP BY 1
ORDER BY 2 DESC;

-- ANSWER: Yorkville West with $81.89