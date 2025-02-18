-- SQL Project - Data Cleaning

-- https://www.kaggle.com/datasets/swaptr/layoffs-2022


select * 
from layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or Blank Values
-- 4. Remove Any Columns

create table layoffs_staging
like layoffs;

insert into layoffs_staging
select * 
from layoffs;

select *
from layoffs_staging;


-- Remove Duplicates
with duplicates_cte as
(
	select *,
    row_number() over (
    partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, country, funds_raised_millions) as `row_number`
    from layoffs_staging
) select *
from duplicates_cte
where `row_number` > 1;

create TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_number` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

drop table layoffs_staging2;

insert layoffs_staging2
select *,
row_number() over (
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, country, funds_raised_millions) as `row_number`
from layoffs_staging;

delete
from layoffs_staging2
where `row_number` > 1;

select *
from layoffs_staging2
where `row_number` > 1;


-- Standardizing the Data
select *
from layoffs_staging2;

select distinct company 
from layoffs_staging2
order by 1;

update layoffs_staging2
set company = trim(company);

select distinct industry
from layoffs_staging2
order by 1;

update layoffs_staging2
set industry = "Crypto"
where industry like "Crypto%";

select distinct country
from layoffs_staging2
order by 1;

select distinct country, 
trim(trailing '.' from country)
from layoffs_staging2;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like "United States%";

select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

alter table layoffs_staging2
modify column `date` date;


-- Null Values or Blank Values
select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2
where industry is null
or industry = '';

select *
from layoffs_staging2
where company = 'Airbnb';

select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2 
	on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update layoffs_staging2
set industry = null
where industry is null or industry = '';

update layoffs_staging2 t1
join layoffs_staging2 t2 
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;


-- Remove Any Columns
alter table layoffs_staging2
drop column `row_number`;

select *
from layoffs_staging2;