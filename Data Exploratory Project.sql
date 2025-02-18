-- exploratory project

-- view all data from the table
select * from layoffs_staging2;

-- total layoffs
select sum(total_laid_off) as total_laid_off from layoffs_staging2;

-- average percentage of layoffs (excluding null values)
select avg(cast(percentage_laid_off as decimal(10,2))) as avg_percentage_laid_off
from layoffs_staging2
where percentage_laid_off is not null;

-- average funds raised by companies that had layoffs
select avg(funds_raised_millions) as avg_funds
from layoffs_staging2
where total_laid_off > 0;

-- analysis by time

-- view all data again for reference
select * from layoffs_staging2;

-- total layoffs grouped by year and month
select sum(total_laid_off) as total_laid_off,
       year(`date`) as `year`,
       month(`date`) as `month_number`,
       monthname(`date`) as `month`
from layoffs_staging2
group by `year`, `month_number`, `month`
order by `year`, `month_number`;

-- find the months with the most and least layoffs
with max_min_layoffs as (
    select monthname(`date`) as `month`, total_laid_off,
           case 
               when total_laid_off = (select max(total_laid_off) from layoffs_staging2) then 'most layoffs'
               when total_laid_off = (select min(total_laid_off) from layoffs_staging2 where total_laid_off > 0) then 'least layoffs'
           end as category
    from layoffs_staging2
)
select month, category
from max_min_layoffs
where category is not null
group by month, category;

-- identify anomalies in monthly layoffs using standard deviation
with t_cte as (
    select sum(total_laid_off) as total_laid_off_per_month, monthname(`date`) as `month`
    from layoffs_staging2
    group by `month`
),
stats as (
    select avg(total_laid_off_per_month) as mean, stddev(total_laid_off_per_month) as std_dev
    from t_cte
)
select t_cte.`month`, t_cte.total_laid_off_per_month,
       case 
           when t_cte.total_laid_off_per_month > (stats.mean + stats.std_dev) then 'anomaly'
           else 'no anomaly'
       end as anomaly_category
from t_cte, stats
order by t_cte.total_laid_off_per_month desc;

-- analysis by industries

-- view all data again
select * from layoffs_staging2;

-- top 5 industries by total layoffs
select industry, sum(total_laid_off) as total_laid_off
from layoffs_staging2
where industry is not null
group by industry
order by total_laid_off desc
limit 5;

-- fraction of total layoffs per industry
with industry_cte as (
    select industry, sum(total_laid_off) as total_laid_off
    from layoffs_staging2
    where industry is not null
    group by industry
)
select industry, 
       (total_laid_off * 100.0 / (select sum(total_laid_off) from layoffs_staging2 where industry is not null)) as fraction
from industry_cte
order by fraction desc;

-- top 5 industries by total funding raised
select industry, sum(funds_raised_millions) as total_funding, sum(total_laid_off) as total_laid_off
from layoffs_staging2
where industry is not null and funds_raised_millions is not null
group by industry
order by total_funding desc
limit 5;

-- analysis by countries

-- view all data again
select * from layoffs_staging2;

-- top 5 countries by total layoffs
select country, sum(total_laid_off) as total_laid_off
from layoffs_staging2
where country is not null
group by country
order by total_laid_off desc
limit 5;

-- categorize countries based on layoff averages
with country_cte as (
    select country, avg(total_laid_off) as average_layoffs
    from layoffs_staging2
    where country is not null
    group by country
),
overall_avg as (
    select avg(total_laid_off) as global_avg
    from layoffs_staging2
    where total_laid_off is not null
)
select c.*, 
       case 
           when c.average_layoffs > o.global_avg then 'higher than average'
           when c.average_layoffs < o.global_avg then 'less than average'
           else 'invalid'
       end as category
from country_cte c
cross join overall_avg o;

-- categorize countries based on layoffs and funding
with country_cte as (
    select country, avg(total_laid_off) as average_layoffs, avg(funds_raised_millions) as average_millions
    from layoffs_staging2
    where country is not null
    group by country
),
overall_avg as (
    select avg(total_laid_off) as global_avg, avg(funds_raised_millions) as global_millions_avg
    from layoffs_staging2
    where total_laid_off is not null and funds_raised_millions is not null
)
select c.country, c.average_layoffs, c.average_millions,
       case 
           when c.average_layoffs > o.global_avg and c.average_millions > o.global_millions_avg then 'high funding, high layoffs'
           when c.average_layoffs <= o.global_avg and c.average_millions > o.global_millions_avg then 'high funding, low layoffs'
           when c.average_layoffs > o.global_avg and c.average_millions <= o.global_millions_avg then 'low funding, high layoffs'
           else 'low funding, low layoffs'
       end as category
from country_cte c
cross join overall_avg o
order by c.average_millions desc, c.average_layoffs desc;

-- analysis by companies

-- view all data again
select * from layoffs_staging2;

-- top 10 companies by total layoffs
select company, sum(total_laid_off) as total_laid_off
from layoffs_staging2
where company is not null
group by company
order by total_laid_off desc
limit 10;

-- check for companies with repeated layoffs
with repeated_cte as (
    select company, total_laid_off, `date`,
           count(*) over (partition by company) as layoff_count
    from layoffs_staging2
    where total_laid_off is not null
)
select company, total_laid_off, `date`,
       case 
           when layoff_count > 1 then 'company made repeated layoffs'
           else 'company made only one layoff'
       end as category
from repeated_cte
order by company, `date`;

-- identify companies that raised large investments before making layoffs
with investment_cte as (
    select company, max(funds_raised_millions) as max_funds, max(`date`) as investment_date
    from layoffs_staging2
    where funds_raised_millions is not null
    group by company
),
layoff_cte as (
    select company, total_laid_off, `date` as layoff_date
    from layoffs_staging2
    where total_laid_off is not null
)
select i.company, i.max_funds, i.investment_date, l.total_laid_off, l.layoff_date,
       timestampdiff(month, i.investment_date, l.layoff_date) as months_after_funding
from investment_cte i
join layoff_cte l on i.company = l.company
where timestampdiff(month, i.investment_date, l.layoff_date) between 1 and 12
order by i.max_funds desc, l.total_laid_off desc;