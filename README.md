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
