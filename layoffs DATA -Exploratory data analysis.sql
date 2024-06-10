project_clean_data 


select * 
from layoffs;


1------remove_duplicate--

create table layoffs_staging
like layoffs;

select *
from layoffs_staging;

insert into layoffs_staging
select *
from layoffs;


select *,
row_number() over (
partition by company, industry, total_laid_off, percentage_laid_off,`date`) AS ROW_NUM
FROM layoffs_staging;


WITH DUPLICATE_CTE AS 
(
select *,
row_number() over (
partition by company,location, industry, 
total_laid_off,percentage_laid_off ,`date`,stage,country,funds_raised_millions) AS ROW_NUM
FROM layoffs_staging
)
select *
FROM DUPLICATE_CTE
WHERE ROW_NUM >1;

SELECT *
FROM layoffs_staging
WHERE ROW_NUM >1;



CREATE TABLE `layoffs_staging` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `ROW_NUM` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



SELECT *
FROM layoffs_staging2;


INSERT INTO layoffs_staging2
select *,
row_number() over (
partition by company,location, industry, 
total_laid_off,percentage_laid_off ,`date`,stage,country,funds_raised_millions) AS ROW_NUM
FROM layoffs_staging;


SET SQL_SAFE_UPDATES = 0;

 dELETE 
FROM layoffs_staging2
WHERE ROW_NUM >1;


SELECT *
FROM layoffs_staging2;



--------2.STANDARIZING_DATA

SELECT DISTINCT (TRIM(company))
from layoffs_staging2;

SELECT company, (TRIM(company))
from layoffs_staging2;


update layoffs_staging2
set company = TRIM(company);


select company,TRIM(company)
from layoffs_staging2;

select distinct industry 
from layoffs_staging2
order by 1;

select *
from layoffs_staging2
where industry like 'Crypto%';

update layoffs_staging2
set industry = 'Crypto%'
where industry like 'Crypto%';


select distinct location 
from layoffs_staging2
order by 1;



select distinct country 
from layoffs_staging2
order by 1;




select distinct country 
from layoffs_staging2
where country like 'United States%'
order by 1;



select distinct country, trim(country) 
from layoffs_staging2
order by 1;


select distinct country, trim(TRAILING '.' FROM country) 
from layoffs_staging2
order by 1;



UPDATE layoffs_staging2 
SET COUNTRY = TRIM(TRAILING '.' FROM country)
WHERE COUNTRY LIKE 'United States%';


SELECT `date` ,
str_to_date(  `date` ,'m%/%d/%Y')
FROM layoffs_staging2;



UPDATE layoffs_staging2
SET `date`= str_to_date(`date`,'m%/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


3--- REMOVE_NULL 

SELECT *
FROM layoffs_staging2 
WHERE total_laid_off is null
and percentage_laid_off is null;

update layoffs_staging2
set industry = null
where industry = ''; 

select distinct industry
from layoffs_staging2
where industry is null
or industry = '';

select * 
from layoffs_staging2 
where company ='Airbnb';

select  t1.industry , t2.industry 
from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
where (t1.industry is null or t1.industry = '') 
and t2.industry is not null;




update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company 
set t1.industry = t2.industry
where (t1.industry is null or t1.industry = '') 
and t2.industry is not null;


update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company 
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;


4---delete_column_that _ i dont_use

delete layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2;


alter table layoffs_staging2
drop column ROW_NUM;


--- exploratory_data_analysis 

select *
from layoffs_staging2
where percentage_laid_off =1
order by funds_raised_millions desc;

select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;


select min(`date`), max(`date`)
from layoffs_staging2;


select country, sum(total_laid_off)
from layoffs_staging2
group by country
order by 2 desc;

select Year(`date`), sum(total_laid_off)
from layoffs_staging2
group by Year(`date`)
order by 1 desc;




select stage, sum(total_laid_off)
from layoffs_staging2
group by stage
order by 2 desc;



select *
from layoffs_staging2;


select company, AVG(percentage_laid_off)
from layoffs_staging2
group by company
order by 2 desc;


-----FOR_ DAY_AND_MONTH_YEAY
select substring(`date`,1,10) AS `MONTH`, sum(total_laid_off)
from layoffs_staging2
WHERE substring(`date`,1,10)  IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

-----FOR_MONTH_YEAY
select substring(`date`,1,7) AS `MONTH`, sum(total_laid_off)
from layoffs_staging2
WHERE substring(`date`,1,7)  IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;



-----FOR_YEAY
select substring(`date`,6,2) AS `MONTH`, sum(total_laid_off)
from layoffs_staging2
WHERE substring(`date`,6,2)  IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;


select substring(`date`,1,7) AS `MONTH`, sum(total_laid_off)
from layoffs_staging2
WHERE substring(`date`,1,7)  IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

WITH ROLLING_TOTAL AS
(
select substring(`date`,1,7) AS `MONTH`, sum(total_laid_off) as total_off
from layoffs_staging2
WHERE substring(`date`,1,7)  IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`,total_off
, SUM(total_off) over(order by `MONTH`) as rolling_total 
FROM ROLLING_TOTAL;



select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;



select company,year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company,YEAR(`date`)
ORDER BY 3 desc;






select company,year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company,YEAR(`date`)
order by 3 desc;

WITH Company_Year (company,years,total_laid_off) as
(
select company,year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company,YEAR(`date`)
),Company_Year_Rank AS
(select*, dense_rank() OVER(partition by YEARS ORDER BY total_laid_off DESC) AS RANKING
from  Company_Year
WHERE YEARS IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
where Ranking <=5;
;
