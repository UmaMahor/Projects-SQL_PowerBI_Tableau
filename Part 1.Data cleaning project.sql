-- SQL Project - Data Cleaning
-- https://www.kaggle.com/datasets/swaptr/layoffs-2022

select *
from layoffs;


-- we will maka a new table called layoffs_staging as we dont want to make changes to raw data. 
create table layoffs_staging
like layoffs;

insert layoffs_staging
select * 
from layoffs;
select * from layoffs_staging;




-- we are creating a query to partition all columns, create a new column named as row_num.
-- this row_num will assign no 2 to duplicate entry

-- syntax for row number
# ROW_NUMBER() OVER ( [PARTITION BY partition_expression] ORDER BY order_expression )

select *,
row_number() over(
partition by company,location,industry,
total_laid_off,percentage_laid_off,`date`,
stage,country,funds_raised_millions) 
as row_num
from layoffs_staging;



-- syntax for CTE
#WITH cte_name AS (SELECT query)
#SELECT *
#FROM cte_name;

With duplicate_cte as
(
select *,
row_number() over(
partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_staging
)

select * 
from duplicate_cte
where row_num >1
;

# lets check first one casper

select *
from layoffs_staging
where company = 'casper'; 
#row 1 and row3 looks duplicate
#we want to remove one of these rows. we dont want to delete both of two rows

With duplicate_cte as
(
select *,
row_number() over(
partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_staging
)

delete
from duplicate_cte
where row_num >1
;
-- we cannot just delete from cte. its giving error
-- we will create a new table staging 2 where we will delete all results with row_num >2

create table layoffs_staging2
like layoffs_staging;
alter table layoffs_staging2
add row_num INT;
select * from layoffs_staging2;
-- this is an emppty table with same column as layoff_staging2  + extra cloumn as row_num 


select * 
from layoffs_staging2;

insert into layoffs_staging2

select *,
row_number() over(
partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_staging;

-- lets see if it worked
select * 
from layoffs_staging2;

-- lets filter results where row_num is >1

select * 
from layoffs_staging2
where row_num >1;

-- showing all results with row_num >1

-- delete all results with row_num>1

delete 
from layoffs_staging2
where row_num >1;


select * 
from layoffs_staging2
where row_num >1;

select * from layoffs_staging2; 
-- we have removed duplicates..its just in the end we will also remove row_num as we dont need it anymore

-- Topic 2: Standardizing data

-- ************Standardizing data***************
select distinct(company) 
from layoffs_staging2;
-- there is extra space in company name for eg included health. so lets make all unifrom bt trim function

select company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);
select * from layoffs_staging2;

-- use of distinct funciton
select industry 
from layoffs_staging2;
-- if we do select industry.. names will repeat .. as you see marketing in row 3 and 4

select distinct(industry) 
from layoffs_staging2;
-- now we dont see two marketing.. we see distinct values

select distinct(industry)
from layoffs_staging2
order by (industry);

-- if you notice we have crypto and crypto currency...this doesnt seem good as its same thing

select industry 
from layoffs_staging2
where industry like '%crypto%';  # it will show all char before after crypto

update layoffs_staging2
SET INDUSTRY = 'Crypto'
WHERE industry = '%crypto%';

select distinct(industry)
from layoffs_staging2;

-- we have looked at company and industry..lets move on to other fields

select distinct(country)
from layoffs_staging2
order by country;  #arranged alphabetically
-- notice united states is ending with . this is not good

SELECT DISTINCT COUNTRY, TRIM( TRAILING '.' FROM COUNTRY)
FROM LAYOFFS_STAGING2
ORDER BY COUNTRY;

update layoffs_staging2
set country = TRIM(TRAILING'.' FROM COUNTRY)
Where country LIKE 'United States%'
;

select distinct(country)
from layoffs_staging2
order by country;
-- now it shows only United States..thats good


-- Lets take a look at date.. click on left side layoff_staging2..drop down cloumn..right click date
-- under information tab u will see this in text format..change to date format

select * from layoffs_staging2;

select `date`,
str_to_date(`date`, '%m/%d/%Y')    #dont use capital M
from layoffs_staging2;


UPDATE layoffs_staging2
set `date` =  str_to_date(`date`, '%m/%d/%Y') ;
-- refresh table from left you will notice date is still in text format..
-- now we can change column data type

ALTER TABLE layoffs_staging2
modify column `date` DATE;
-- Now refresh you will see date data type

select * from layoffs_staging2;


-- ****Topic 3: Null/Blank Values*****

select *
from layoffs_staging2
where total_laid_off= null;  # no result we have to say is null

select *
from layoffs_staging2
where total_laid_off is null;

select *
from layoffs_staging2
where total_laid_off is Null  #Showing only total_laid_off null
and percentage_laid_off is NULL;  #Showing both total and percentage null

-- lets take a look at data once again in other columns..lets check industry it is having misisng values and null
update layoffs_staging2 
set industry =null
WHERE INDUSTRY = '';

select *
from layoffs_staging2
where industry is null
or industry = '';

select * 
from layoffs_staging2
where company = 'airbnb';

-- we see that airbnb fro SF bay area in under travel industry.. but one row is not showing that
-- it should show travel industry for all airbnb..
-- how do we know which compnaies are having similiar issues like airbnb
-- we can write a query to identify those companies


-- we are using join function within same table


select *
from layoffs_staging2 t1
join layoffs_staging2 t2
 ON t1.company= t2.company
 and t1.location = t2.location
where t1.industry is NULL
AND t2.industry is not NULL;
-- nothing in outpout ..lets remove location

select *
from layoffs_staging2 t1
join layoffs_staging2 t2
 ON t1.company= t2.company
where t1.industry is NULL
AND t2.industry is not NULL;

-- still nothing lets also include blank with null

select *
from layoffs_staging2 t1
join layoffs_staging2 t2
 ON t1.company= t2.company
where (t1.industry is NULL )
AND t2.industry is not NULL ;

select t1.industry,t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
 ON t1.company= t2.company
where (t1.industry is NULL or t1.industry = '')
AND t2.industry is not NULL ;


-- now lets update industry in blank or null field


update layoffs_staging2 t1 #updating this table

join layoffs_staging2 t2
 ON t1.company= t2.company #joining same table where company equals

 set t1.industry=t2.industry #saying that it should have same industry
 
where (t1.industry is NULL or t1.industry = '')
AND t2.industry is not NULL ;


SELECT *
FROM layoffs_staging2
WHERE COMPANY= 'AIRBNB';

SELECT *
FROM layoffs_staging2
WHERE COMPANY= 'bally';


SELECT *
FROM layoffs_staging2
WHERE COMPANY= 'carvana';

select * from layoffs_staging2;
-- now industry dont have null values anymore

select *
from layoffs_staging2
where total_laid_off is Null  
and percentage_laid_off is NULL;  

delete 
from layoffs_staging2
where total_laid_off is Null  
and percentage_laid_off is NULL;  

select * from layoffs_staging2 ;


-- ***** Topic 4 is removing column********
-- lets get rid of row number we dont need that anymore
alter table layoffs_staging2
drop column row_num;

select * from layoffs_staging2 ;
