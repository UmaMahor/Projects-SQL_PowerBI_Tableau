-- Exploratory Data Analysis

select * from layoffs_staging2;

-- Maximum value under total laid off column 
select max(total_laid_off)
from layoffs_staging2;

-- Maximum value under total laid off and percentage laid off column showing how big these layoffs were
select max(total_laid_off) ,max(percentage_laid_off)
from layoffs_staging2;


-- Which company had percentage laid off =1 , meaning the company which fired 100 % employees in decreasing order based on no of total laid off 
select * from 
layoffs_staging2
where percentage_laid_off = 1
order by total_laid_off desc;

-- Companies who fired 100 percent of the employees arranged in decreasing order based on funds raised by them (company size)
select * from 
layoffs_staging2
where percentage_laid_off = 1
order by funds_raised_millions desc;



-- Showing company name and total laid off.This is arranged in desc order based on people laid off
select company, sum(total_laid_off) 
from layoffs_staging2
group by company
order by sum(total_laid_off) desc;


-- This time we are grouping by industry name to see what was the effect at industry level
select industry, sum(total_laid_off) 
from layoffs_staging2
group by industry
order by sum(total_laid_off) desc;

-- Total laid off at country level  , arranged in desc order based on total laid off

select country, sum(total_laid_off) 
from layoffs_staging2
group by country
order by sum(total_laid_off) desc;

-- Shows total laid off based on the individual date
select `date`, sum(total_laid_off) 
from layoffs_staging2
group by `date`
order by `date` desc;

-- Showing total laid off based on individual year 
select year(`date`), sum(total_laid_off) 
from layoffs_staging2
group by year(`date`)
order by year(`date`) desc;

-- Showing total laid off based on company size

select stage, sum(total_laid_off) 
from layoffs_staging2
group by stage
order by stage desc;

-- Showing total laid off based on company size, this time total laid off is in desc order
select stage, sum(total_laid_off) 
from layoffs_staging2
group by stage
order by sum(total_laid_off) desc;


select company, sum(percentage_laid_off) 
from layoffs_staging2
group by company
order by sum(percentage_laid_off) desc;

-- ------------------------------------------------------------------------------------------------------------

-- Lets understand use of ranking by using Dense_Rank function
-- But first lets see the basic data 

-- Data 1: Company with highest total laid off 
select company, sum(total_laid_off) 
from layoffs_staging2
group by company
order by sum(total_laid_off) desc;

-- Data 2:Company with highest total laid off + showing the respective year

select company,year(`date`), sum(total_laid_off) 
from layoffs_staging2
group by company,year(`date`)
order by sum(total_laid_off) desc;

-- How can we know that in year 2022, which company had highest laid off
-- based on above query we can say in year 2022, 
-- Meta fired 11000 and amazon fired 10150 so if we have to rank them Meta is rank 1 and amazon is rank 2
-- Lets create a query to see this type of result for every year

with company_year (company, years,total_laid_off) as
(
select company,year(`date`), sum(total_laid_off) 
from layoffs_staging2
group by company,year(`date`)
order by sum(total_laid_off) desc
)
select *,
dense_rank() over (partition by years order by total_laid_off desc) as Ranking
from company_year
where years is not null
order by Ranking asc
;

-- Company "UBER" in the "YEAR 2020" laid off "7525" which was highest of year 2020 and hence "UBER" is Ranked 1
