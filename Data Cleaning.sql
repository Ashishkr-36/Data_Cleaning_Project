-- Data Cleaning --

select *
from layoffs
;

-- Remove Duplicates
-- Standardize the Data
-- Null values or blank values
-- Remove Any Columns

create table layoffs_staging
like layoffs
;

select *
from layoffs_staging
;

insert layoffs_staging
select *
from layoffs
;

select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, 'date') as row_num
from layoffs_staging
;

with duplicate_cte as
(
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, 'date',
stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1
;


delete
from layoffs_staging
where company = 'Casper'
;


CREATE TABLE `layoffs_staging2` (
  `company` TEXT,
  `location` TEXT,
  `industry` TEXT,
  `total_laid_off` INT DEFAULT NULL,
  `percentage_laid_off` TEXT,
  `date` TEXT,
  `stage` TEXT,
  `country` TEXT,
  `funds_raised_millions` INT DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging2
;

insert into layoffs_staging2
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, 
'date', stage, country, funds_raised_millions) as row_num
from layoffs_staging
;

select *
from layoffs_staging2
where row_num > 1;

delete
from layoffs_staging2
where row_num > 1;

select *
from layoffs_staging2
where row_num > 1;

select *
from layoffs_staging2
;

--  Standardizing data --

select company, trim(company)
from layoffs_staging2
;

update layoffs_staging2
set company = Trim(company);


select distinct industry
from layoffs_staging2
order by 1;

select *
from layoffs_staging2
where industry like 'Crypto%';


update layoffs_staging2
set industry= 'Crypto'
where industry like 'Crypto%';

select distinct industry
from layoffs_staging2
;


select distinct location
from layoffs_staging2
order by 1;

select distinct country
from layoffs_staging2
order by 1;

select *
from layoffs_staging2
where country like 'United States%'
order by 1;

select distinct country, trim(Trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country =  trim(Trailing '.' from country)
where country like 'United States%';


select `date`
from layoffs_staging2
;

select `date`,
str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2
;

update layoffs_staging2
set `date` = str_to_date(`date`,'%m/%d/%Y')
;

alter table layoffs_staging2
modify column `date` Date
;
 
 select *
 from layoffs_staging2
 where total_laid_off is NULL
 and percentage_laid_off is NULL
 ;
 
 update layoffs_staging2
 set industry = null
 where industry = ''
 ;
 
 select *
 from layoffs_staging2
 where industry is NULL
 or industry = ''
 ;
 
 select *
 from layoffs_staging2
 where company like 'Bally%'
 ;
 
 select t1.industry, t2.industry
 from layoffs_staging2 t1
 join layoffs_staging2 t2
	on t1.company = t2.company
where (t1.industry is NULL OR t1.industry = '')
and t2.industry is not null
 ;
 
 update layoffs_staging2 t1
 join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is NULL 
and t2.industry is not null
;
 
 select *
 from layoffs_staging2
 where total_laid_off is null
 and percentage_laid_off is null
 ;
 
 delete
 from layoffs_staging2
 where total_laid_off is null
 and percentage_laid_off is null
 ;
 
  select *
 from layoffs_staging2
 ;
 
 alter table layoffs_staging2
 drop column row_num
 ;
 
 select *
 from world_layoffs.layoffs_staging2;
 