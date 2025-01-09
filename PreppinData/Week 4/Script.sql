with pre_pivot as (
with cte as (
select *, 'December' as Table_name from pd2023_wk04_december
UNION ALL
select *, 'April' as Table_name from pd2023_wk04_april
UNION ALL
select *, 'August' as Table_name from pd2023_wk04_august
UNION ALL
select *, 'February' as Table_name from pd2023_wk04_february
UNION ALL
select *, 'January' as Table_name from pd2023_wk04_january
UNION ALL
select *, 'July' as Table_name from pd2023_wk04_july
UNION ALL
select *, 'June' as Table_name from pd2023_wk04_june
UNION ALL
select *, 'March' as Table_name from pd2023_wk04_march
UNION ALL
select *, 'May' as Table_name from pd2023_wk04_may
UNION ALL
select *, 'November' as Table_name from pd2023_wk04_november
UNION ALL
select *, 'October' as Table_name from pd2023_wk04_october
UNION ALL
select *, 'September' as Table_name from pd2023_wk04_september
)

select 
id
, demographic
, value
, date_from_parts(2023, date_part('month',date(Table_name,'MMMM')), JOINING_DAY) as Joining_Date 
from cte
)
, post_pivot as (
select 
id
, joining_date
, ethnicity
, date_of_birth::date as date_of_birth
, account_type
, row_number() over(partition by id order by joining_date) as rn
from pre_pivot
pivot(max(value) for demographic in ('Ethnicity', 'Account Type', 'Date of Birth')) as P
(id
, joining_date
, ethnicity
, account_type
, date_of_birth)
)

select *
from post_pivot
where rn = 1
