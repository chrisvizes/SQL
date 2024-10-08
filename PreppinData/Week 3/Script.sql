WITH TARGETS AS(
SELECT 
		ONLINE_OR_IN_PERSON 
		, TO_NUMBER(REPLACE(QUARTER, 'Q', '')) AS QUARTER
		, QUARTERLY_TARGET
FROM 
		TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK03_TARGETS 
	UNPIVOT(
		QUARTERLY_TARGET FOR QUARTER IN (Q1, Q2, Q3, Q4)
	) AS PIVOTED_TARGETS
),
TRANSACTIONS AS(
SELECT
	SUM(VALUE) AS VALUE 
	, CASE ONLINE_OR_IN_PERSON WHEN 1 THEN 'Online' WHEN 2 THEN 'In-Person' ELSE 'ERROR' END AS ONLINE_OR_IN_PERSON 
	, DATE_PART('QUARTER', TO_DATE(TRANSACTION_DATE,'DD/MM/YYYY HH24:MI:SS')) AS QUARTER
FROM
	TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK01 
WHERE TRANSACTION_CODE LIKE 'DSB%'
GROUP BY QUARTER, ONLINE_OR_IN_PERSON
)
SELECT TRANSACTIONS.QUARTER
	, TRANSACTIONS.ONLINE_OR_IN_PERSON
	, TRANSACTIONS.VALUE
	, TARGETS.QUARTERLY_TARGET
	, TRANSACTIONS.VALUE - TARGETS.QUARTERLY_TARGET AS "Variance to target"
FROM TRANSACTIONS
INNER JOIN TARGETS
	WHERE TRANSACTIONS.QUARTER = TARGETS.QUARTER
	AND TRANSACTIONS.ONLINE_OR_IN_PERSON = TARGETS.ONLINE_OR_IN_PERSON;
