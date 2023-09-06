--SELECT * FROM news
--SELECT grade_lvl.title, category,frequency, grade_level, country, publish_date, article_link FROM grade_lvl

--Set up CTE
with freqgrade as (
	SELECT grade_lvl.title as Title, 
		frequency,
	    frequency * grade_level as frequency_of_words,
		category,
	   grade_level,
	   article_link,
		publish_date,
		author,
		country
	FROM grade_lvl
	Join news on news.title = grade_lvl.title
	where frequency != 0 or grade_level != Null
	GROUP BY grade_level, grade_lvl.title, category, article_link, frequency, publish_date,author,country
	order by grade_lvl.title
    ) 
	
	
--Select avgerage grade level by title 
select 
	freqgrade.Title,
	sum(freqgrade.frequency_of_words)/sum(freqgrade.frequency) as avg_grade_level,
	freqgrade.category,
	freqgrade. article_link,
	freqgrade.publish_date,
		freqgrade.author,
		freqgrade.country
from 
   freqgrade
Group by freqgrade.Title, 
	  freqgrade.category,
	  freqgrade. article_link,
	  freqgrade.publish_date,
		freqgrade.author,
		freqgrade.country
order by freqgrade.Title		
	
	

--Select avgerage grade level by Category 
select 
	freqgrade.category,
	sum(freqgrade.frequency_of_words)/sum(freqgrade.frequency) as avg_grade_level
from 
   freqgrade
Group by freqgrade.category
order by freqgrade.category

