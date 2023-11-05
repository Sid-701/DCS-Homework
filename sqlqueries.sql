--QUESTION 1



SELECT animal_type, count(*) FROM animal GROUP BY animal_type;


--QUESTION 2



WITH AnimalsWithMultipleOutcomes AS (
    SELECT
        animal_id
    FROM outcome_events
    GROUP BY animal_id
    HAVING COUNT(*) > 1
)

SELECT COUNT(*) AS "Animals with More than 1 Outcome"
FROM AnimalsWithMultipleOutcomes;


--QUESTION 3


WITH MonthNames AS (
    SELECT
        generate_series(1, 12) AS month_number,
        ARRAY['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'] AS month_names
)

SELECT
    mn.month_names AS month_name,
    COUNT(*) AS month_count
FROM outcome_events AS oe
JOIN MonthNames AS mn ON EXTRACT(MONTH FROM oe.datetime) = mn.month_number
GROUP BY mn.month_names
ORDER BY month_count DESC
LIMIT 5;




   


--question 4


WITH CatAdoptions AS (
    SELECT
        a.animal_type,
        DATE_PART('year', AGE(oe.datetime, a.date_of_birth)) AS age_in_years,
        oe.outcome_type 
    FROM
        animal AS a
    JOIN
        outcome_events AS oe ON a.animal_id = oe.animal_id
    WHERE oe.outcome_type = 'Adoption' AND a.animal_type = 'Cat'
)

SELECT
    CASE
        WHEN age_in_years < 1 THEN 'Kitten'
        WHEN age_in_years > 10 THEN 'Senior Cat'
        ELSE 'Adult'
    END AS age_category,
    outcome_type,
    COUNT(*) AS count
FROM CatAdoptions
GROUP BY age_category, outcome_type;


--question 5

WITH DailyOutcomes AS (
    SELECT
        DATE(datetime) AS "Date",
        COUNT(*) AS "Daily Outcomes"
    FROM outcome_events
    GROUP BY DATE(datetime)
)

SELECT
    "Date",
    "Daily Outcomes",
    SUM("Daily Outcomes") OVER (ORDER BY "Date") AS "Cumulative Total Outcomes"
FROM DailyOutcomes
ORDER BY "Date";
