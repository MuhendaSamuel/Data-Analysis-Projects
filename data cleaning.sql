-- Data Cleaning


SELECT * 
FROM world_layoffs.layoffs;

-- 1. Remove Duplicates
-- 2. Standardise the Data
-- 3. Null Values or blank values
-- 4. Remove any columns



CREATE TABLE layoffs_staging
LIKE layoffs ;


SELECT * 
FROM world_layoffs.layoffs_staging;

INSERT world_layoffs.layoffs_staging
SELECT *
FROM layoffs;


SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company , industry, total_laid_off , percentage_laid_off , 'date') AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company , location,
 industry, total_laid_off , percentage_laid_off , 'date' , stage
 , country , funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1 ;


SELECT *
FROM layoffs_staging
WHERE company = 'Casper' ;


WITH duplicate_cte AS
(
SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company , location,
 industry, total_laid_off , percentage_laid_off , 'date' , stage
 , country , funds_raised_millions) AS row_num
FROM layoffs_staging
)
DELETE 
FROM duplicate_cte
WHERE row_num > 1 ;



CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2 
WHERE row_num > 1
 ;

INSERT INTO layoffs_staging2
SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company , location,
 industry, total_laid_off , percentage_laid_off , 'date' , stage
 , country , funds_raised_millions) AS row_num
FROM layoffs_staging;


DELETE
FROM layoffs_staging2 
WHERE row_num > 1
 ;
SELECT *
FROM layoffs_staging2 
WHERE row_num > 1
 ;
 
 SET SQL_SAFE_UPDATES = 0;
 DELETE 
 FROM layoffs_staging2 
 WHERE row_num > 1;
 
 SELECT *
FROM layoffs_staging2 
WHERE row_num > 1;

SET SQL_SAFE_UPDATES = 1;

SELECT *
FROM layoffs_staging2 ;

-- Standardising Data

SELECT company , trim(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT distinct country , trim(trailing '.' from country) 
FROM layoffs_staging2
ORDER BY 1;

SELECT `date`,
str_to_date(`date`, `%m/%d/%Y`)
FROM layoffs_staging2;


UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET country = trim(trailing '.' from country)
where country like 'United States%';

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');


UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

select `date` ,
STR_TO_DATE(`date`, '%m/%d/%Y')
from layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT company, `date`, total_laid_off
FROM layoffs_staging2
ORDER BY `date` DESC;
