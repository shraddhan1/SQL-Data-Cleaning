SELECT * FROM world_layoffs.layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data OR Fix structural errors
-- 3. Null Values or blank values OR Filter unwanted outliers
-- 4. Remove Any Columns OR Handle missing data
-- 5. Validate your data

CREATE TABLE layoffs_stagging LIKE world_layoffs.layoffs;

SELECT * FROM layoffs_stagging;

INSERT layoffs_stagging SELECT * FROM world_layoffs.layoffs;

SELECT *,
ROW_NUMBER() 
OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_stagging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() 
OVER(
PARTITION BY company,location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_stagging
)
SELECT * FROM duplicate_cte WHERE row_num > 1;

SELECT * FROM layoffs_stagging WHERE company = 'Casper';

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() 
OVER(
PARTITION BY company,location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_stagging
)
DELETE FROM duplicate_cte WHERE row_num > 1;

CREATE TABLE `layoffs_staging2` (
`company` text, 
`location` text, 
`industry` text, 
`total_laid_off` int, 
`percentage_laid_off` text, 
`date` text, 
`stage` text, 
`country` text, 
`funds_raised_millions` int DEFAULT NULL,
`row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * FROM layoffs_staging2
WHERE row_num > 1;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() 
OVER(
PARTITION BY company,location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_stagging;


DELETE FROM layoffs_staging2
WHERE row_num > 1;

-- Standardizing Data
select * from layoffs_staging2;

SELECT DISTINCT company,(TRIM(company)) FROM layoffs_staging2;

UPDATE layoffs_staging2 SET company = TRIM(company);

SELECT DISTINCT industry FROM layoffs_staging2
ORDER BY 1;

SELECT * FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2 SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT location FROM layoffs_staging2 ORDER BY 1;

SELECT DISTINCT country FROM layoffs_staging2 ORDER BY 1;

SELECT * FROM layoffs_staging2 WHERE country
LIKE 'United States%'
ORDER BY 1;

SELECT DISTINCT country, TRIM(country) 
FROM layoffs_staging2 ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2 ORDER BY 1;

UPDATE layoffs_staging2 SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT `date`, 
(`date`, '%m/%d/%Y') FROM layoffs_staging2;

UPDATE layoffs_staging2 SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2 MODIFY COLUMN `date` DATE;

SELECT * FROM layoffs_staging2; 

SELECT * FROM layoffs_staging2 
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

UPDATE layoffs_staging2 SET industry = null
WHERE industry = '';

SELECT DISTINCT industry FROM layoffs_staging2;

SELECT * FROM layoffs_staging2
WHERE industry IS NULL OR industry = '';

SELECT * FROM layoffs_staging2
WHERE company LIKE 'Bally%';

SELECT * FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
    WHERE (t1.industry IS NULL OR t1.industry = '')
    AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    SET t1.industry = t2.industry
    WHERE (t1.industry IS NULL OR t1.industry = '')
    AND t2.industry IS NOT NULL;

SELECT * FROM layoffs_staging2;

SELECT * FROM layoffs_staging2 
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE FROM layoffs_staging2 
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging2 DROP COLUMN row_num;

-- Data Exploring Analysis---------------------------
SELECT * FROM layoffs_staging2;

-- Looking at Percentage to see how big these layoffs were
SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off)
FROM layoffs_staging2
WHERE  percentage_laid_off IS NOT NULL;

-- Which companies had 1 which is basically 100 percent of they company laid off
SELECT * FROM layoffs_staging2
WHERE percentage_laid_off = 1;

SELECT * FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

-- if we order by funcs_raised_millions we can see how big some of these companies were
SELECT * FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- Companies with the biggest single Layoff
SELECT company, total_laid_off
FROM layoffs_staging2
ORDER BY 2 DESC
LIMIT 5;
-- now that's just on a single day

-- Companies with the most Total Layoffs
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT MIN(`date`), MAX(`date`) 
FROM layoffs_staging2;

-- by location
SELECT location, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY location
ORDER BY 2 DESC;

-- by industry
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- by country
-- this it total in the past 3 years or in the dataset
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT `date`, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY `date`
ORDER BY 1 DESC;

SELECT YEAR(date), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY YEAR(date)
ORDER BY 1 DESC;


SELECT industry, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;


SELECT stage, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

SELECT company,AVG(percentage_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT substring(`date`, 6, 2) AS MONTH, SUM(total_laid_off)
FROM layoffs_staging2 
GROUP BY `MONTH`;

SELECT substring(`date`, 1, 7) AS MONTH, SUM(total_laid_off)
FROM layoffs_staging2 
GROUP BY `MONTH`;

SELECT substring(`date`, 1, 7) AS MONTH, SUM(total_laid_off)
FROM layoffs_staging2 
WHERE substring(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

WITH Rolling_Total AS
(
SELECT substring(`date`, 1, 7) AS MONTH, SUM(total_laid_off)
FROM layoffs_staging2 
WHERE substring(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)


SELECT `MONTH`, total_off,
SUM(total_off) OVER (ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total;


SELECT company, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT company,YEAR(`date`), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY company,YEAR(`date`)
ORDER BY company ASC;

SELECT company,YEAR(`date`), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY company,YEAR(`date`)
ORDER BY 3 DESC;

# Understand
WITH Comany_Year (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company,YEAR(`date`)
), Company_Year_Rank AS
(SELECT *,DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Comany_Year
WHERE years IS NOT NULL
)
SELECT * FROM Company_Year_Rank
WHERE Ranking <= 5;














