ALTER TABLE shelter1000_new RENAME TO outcomes;

SELECT animal_id, name FROM outcomes type WHERE type = "Cat";

--Question 1: How many animals of each type have outcomes?
SELECT type, COUNT(*) AS ct 
FROM outcomes 
GROUP BY type;


--Question 2: How many animals are there with more than 1 outcome?
WITH animal_type_count AS(
	SELECT type, COUNT(*) as ct 
	FROM outcomes 
	GROUP BY type 
	HAVING ct>1
)
SELECT COUNT(*) FROM animal_type_count;


--Question 3: What are the top 5 months for outcomes?
--SELECT * FROM outcomes;
ALTER TABLE outcomes 
	ADD Month;
UPDATE outcomes 
	SET MONTH = (CASE
		WHEN month_year like "Jan%" THEN 'January'
		WHEN month_year like "Feb%" THEN 'February'
		WHEN month_year like "Mar%" THEN 'March'
		WHEN month_year like "Apr%" THEN 'April'
		WHEN month_year like "May%" THEN 'May'
		WHEN month_year like "Jun%" THEN 'June'
		WHEN month_year like "Jul%" THEN 'July'
		WHEN month_year like "Aug%" THEN 'August'
		WHEN month_year like "Sep%" THEN 'September'
		WHEN month_year like "Oct%" THEN 'October'
		WHEN month_year like "Nov%" THEN 'November'
		WHEN month_year like "Dec%" THEN 'December'
	END
	);
--SELECT * FROM outcomes
WITH month_ct AS(
	SELECT Month, count(*) 
	FROM outcomes 
	GROUP BY Month
	ORDER BY count(*) desc
)
SELECT Month from month_ct LIMIT 5;


--Question 4: A Kitten is a cat who is less than 1 year old. A Senior Cat is a cat who is over 10 years old.
-- An Adult cat is between 1 and 10. 
-- What is the total number of kittens, adults, and seniors, whose outcome is Adopted?

--Something is funky with what I have tried, but this method should work:
--1. Case when transform DOB in a new column for the kitten/adult/senior discretization.
--2. Filter outcome for just adoption, group by the new column of kitten/adult/senior
--3. Get the count(*) of each group.

--Question 5: For each date, what is the cumulatibe total of outcomes up to and including this date?

--While not pretty, we can order by date, add a new column that just takes the row number, 
-- then for each date make all copies of that date have the maximum number associated with it.

