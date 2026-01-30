-- Objective
-- Q1 Does any table have missing values or duplicates? If yes how would you handle it ?

/*with duplicates as
(select *, row_number() over(partition by invoice_id, track_id,unit_price,quantity order by invoice_line_id ) as rn
from invoice_line ),

dups as
(select *, row_number() over(partition by name order by playlist_id ) as rn2 
from playlist)

delete from invoice_line where invoice_line_id in (select distinct(invoice_line_id) from duplicates where rn>1);
delete from playlist where playlist_id in (select distinct(playlist_id) from dups where rn2>1);
delete from playlist_track where playlist_id in (select distinct(playlist_id) from dups where rn2>1);*/

-- Q2 Find the top-selling tracks and top artist in the USA and identify their most famous genres.

with track_sales as
(select i.billing_country, il.track_id, t.name as track_name, il.quantity * il.unit_price as revenue, 
g.genre_id, g.name as genre_name, ar.artist_id, ar.name as artist_name
from invoice i join invoice_line il on i.invoice_id = il.invoice_id
join track t on il.track_id = t.track_id
join genre g on t.genre_id = g.genre_id
join album a on t.album_id = a.album_id
join artist ar on a.artist_id = ar.artist_id
where i.billing_country = "USA")

select track_name, sum(revenue) as track_revenue from track_sales group by track_id, track_name order by track_revenue desc limit 1;
select artist_name, sum(revenue) as artist_revenue from track_sales group by artist_id, artist_name order by artist_revenue desc limit 1;
select genre_name, sum(revenue) as genre_revenue from track_sales group by genre_id, genre_name order by genre_revenue desc limit 1;

-- Q3 What is the customer demographic breakdown (age, gender, location) of Chinook's customer base?
select country, count(customer_id) as customer_count
from customer
group by country order by customer_count desc;

-- Q4 Calculate the total revenue and number of invoices for each country, state, and city:
select billing_country, billing_state, billing_city,
sum(total) as total_revenue, count(invoice_id) as number_of_invoices
from invoice
group by billing_country, billing_state, billing_city
order by billing_country, total_revenue, number_of_invoices;

select billing_country, billing_city,
sum(total) as total_revenue, count(invoice_id) as number_of_invoices
from invoice
group by billing_country, billing_city
order by total_revenue desc, number_of_invoices limit 10;

-- Q5 Find the top 5 customers by total revenue in each country
with top_customers as
(select i.billing_country, c.customer_id, concat(c.first_name," ", c.last_name) as full_name, sum(total) as revenue,
rank() over(partition by i.billing_country order by sum(total) desc) as ranking
from customer c join invoice i
on c.customer_id = i.customer_id
group by i.billing_country, c.customer_id, concat(c.first_name," ", c.last_name))

select *
from top_customers where ranking <= 5;

-- Q6 Identify the top-selling track for each customer
with track_rev as
(select concat(c.first_name," ",c.last_name) as full_name, t.name as track_name, sum(il.unit_price*il.quantity) as revenue,
rank() over(partition by c.customer_id order by sum(il.unit_price*il.quantity)) as rnk
from customer c join invoice i on c.customer_id = i.customer_id
join invoice_line il on i.invoice_id = il.invoice_id
join track t on il.track_id = t.track_id
group by c.customer_id, concat(c.first_name," ",c.last_name), il.track_id, t.name)

select * 
from track_rev where rnk=1;

-- Q7 Are there any patterns or trends in customer purchasing behavior 
-- (e.g., frequency of purchases, preferred payment methods, average order value)?
select year(i.invoice_date) as purchase_year, month(i.invoice_date) as purchase_month, 
count(distinct i.invoice_id) as sales_count, sum(il.quantity*il.unit_price) as revenue_gen,
count(il.invoice_line_id) as tracks_bought
from invoice i join invoice_line il on i.invoice_id = il.invoice_id
group by year(i.invoice_date), month(i.invoice_date);

-- Q8 What is the customer churn rate?
with purchase_timeline as
(select customer_id, date(invoice_date) as purchase_date,
lead(date(invoice_date)) over(partition by customer_id) as next_purchase_date,
date_add(date(invoice_date), interval 1 year) as churn_threshold,
min(date(invoice_date)) over(partition by customer_id order by invoice_date) as first_date,
max(date(invoice_date)) over() as table_last_purchase
from invoice),

first_churn as
(select customer_id, min(churn_threshold) as first_churn_date
from purchase_timeline
where churn_threshold < coalesce(next_purchase_date,table_last_purchase)
group by customer_id),

first_purchase as
(select customer_id, min(first_date) as first_purchase_date
from purchase_timeline group by customer_id),

years as
(select distinct year(invoice_date) as purchase_year from invoice),

active_customers_yearly as
(SELECT y.purchase_year, COUNT(DISTINCT fp.customer_id) AS active_customers
FROM years y JOIN first_purchase fp
ON fp.first_purchase_date < DATE(CONCAT(y.purchase_year, '-01-01'))
LEFT JOIN first_churn fc ON fp.customer_id = fc.customer_id
WHERE fc.first_churn_date IS NULL OR 
fc.first_churn_date >= DATE(CONCAT(y.purchase_year, '-01-01'))
GROUP BY y.purchase_year
ORDER BY y.purchase_year)

select purchase_year as year, round((churned_customers*100/active_customers),2) as churn_rate
from active_customers_yearly acy join
(select year(first_churn_date) as churn_year, count(customer_id) as churned_customers
from first_churn group by year(first_churn_date)) ccy 
on acy.purchase_year = ccy.churn_year
order by purchase_year;

-- Q9 Calculate the percentage of total sales contributed by each genre in the USA and identify the best-selling genres and artists.
with genre_sales as
(select distinct(g.name) as genre_name,
sum(il.quantity*il.unit_price) over(partition by g.genre_id) as genre_revenue,
sum(il.quantity*il.unit_price) over(rows between unbounded preceding and unbounded following) as total_revenue
from invoice i join invoice_line il on i.invoice_id = il.invoice_id
join track t on il.track_id= t.track_id
join album al on al.album_id = t.album_id
join artist a on a.artist_id = al.artist_id
right join genre g on t.genre_id = g.genre_id
where i.billing_country ="USA")

-- select genre_name, round(genre_revenue*100/total_revenue,2) as genre_percent
-- from genre_sales order by genre_percent desc;

-- Q11 Rank genres based on their sales performance in the USA
-- It also uses above CTE
select genre_name, genre_revenue, rank() over(order by genre_revenue desc) as ranking
from genre_sales;

-- Q10 Find customers who have purchased tracks from at least 3 different genres
select c.customer_id, concat(c.first_name," ",c.last_name) as full_name
from customer c join invoice i on c.customer_id = i.customer_id
join invoice_line il on i.invoice_id = il.invoice_id
join track t on il.track_id = t.track_id
group by c.customer_id, concat(c.first_name," ",c.last_name)
having count(distinct t.genre_id) >= 3;

-- Q12 Identify customers who have not made a purchase in the last 3 months
with last_purchase_date as
(select distinct c.customer_id, concat(c.first_name," ", c.last_name) as full_name,
max(i.invoice_date) over(partition by c.customer_id) as latest_date,
max(i.invoice_date) over(rows between unbounded preceding and unbounded following) as last_date
from customer c 
join invoice i on c.customer_id = i.customer_id)

select customer_id, full_name
from last_purchase_date
where date_add(latest_date, interval 3 month) < last_date;


-- Subjective Questions

-- Q1 Recommend the three albums from the new record label that should be prioritised
-- for advertising and promotion in the USA based on genre sales analysis.
with albums_usa as
(select t.album_id, g.genre_id, g.name as genre_name, t.track_id, il.quantity, il.unit_price
from invoice i join invoice_line il on i.invoice_id = il.invoice_id
join track t on il.track_id = t.track_id
join genre g on t.genre_id = g.genre_id
where i.billing_country="USA"),

genre_sales as
(select genre_id, genre_name, sum(quantity*unit_price) as revenue
from albums_usa
group by genre_id, genre_name order by revenue desc limit 1),

valid_albums as
(select a.album_id, a.title, g.genre_id, g.name as genre_name, count(*) as track_count
from album a join track t on a.album_id = t.album_id
join genre g on t.genre_id = g.genre_id
where a.album_id not in (select distinct album_id from albums_usa) and
g.genre_id in (select genre_id from genre_sales)
group by a.album_id, a.title, g.genre_id, g.name)

select t.album_id, va.title, sum(il.quantity*il.unit_price) as revenue
from track t join invoice_line il on t.track_id = il.track_id
join valid_albums va on t.album_id = va.album_id
group by t.album_id, va.title order by revenue desc limit 3;

-- Q2 Determine the top-selling genres in countries other than the USA and identify any commonalities or differences.
with country_genre as
(select i.billing_country as country, g.genre_id, g.name as genre_name, sum(il.quantity*il.unit_price) as revenue,
rank() over(partition by i.billing_country order by sum(il.quantity*il.unit_price) desc) as ranking
from invoice i join invoice_line il on i.invoice_id = il.invoice_id
join track t on il.track_id = t.track_id
join genre g on t.genre_id = g.genre_id
where i.billing_country!="USA"
group by i.billing_country, g.genre_id, g.name)

select *
from country_genre
where ranking = 1;

-- Q3 Customer Purchasing Behavior Analysis: 
-- How do the purchasing habits (frequency, basket size, spending amount) of long-term customers differ from those of new customers?
-- What insights can these patterns provide about customer loyalty and retention strategies?
with cust_purchase_behav as
(select i.customer_id, i.invoice_date, i.invoice_id, count(il.invoice_line_id) as total_track, i.total as spending,
min(i.invoice_date) over(partition by i.customer_id) as first_purchase
from invoice i join invoice_line il
on i.invoice_id = il.invoice_id
group by i.customer_id, i.invoice_date, i.invoice_id
order by customer_id),

cust_type as
(select customer_id, date(invoice_date) as inv_date, total_track, spending,
case
when invoice_date < date_add(first_purchase, interval 12 month) then "new"
else "old" end as classify
from cust_purchase_behav)

select customer_id, classify, count(*)/(timestampdiff(month, min(inv_date), max(inv_date)) +1) as frequency,
round(avg(total_track),2) as avg_basket_size, round(avg(spending),2) as avg_amount
from cust_type
group by customer_id, classify;

-- Q4 Product Affinity Analysis: 
-- Which music genres, artists, or albums are frequently purchased together by customers?
-- How can this information guide product recommendations and cross-selling initiatives?
with invoice_details as
(select i.invoice_id, i.customer_id, il.invoice_line_id, t.track_id, a.album_id, a.title, ar.artist_id,
ar.name as artist_name, g.genre_id, g.name as genre_name
from invoice i join invoice_line il on i.invoice_id = il.invoice_id
join track t on t.track_id = il.track_id
join album a on t.album_id = a.album_id
join artist ar on a.artist_id = ar.artist_id
join genre g on t.genre_id = g.genre_id),

-- Genre Pairs
diff_genre as
(select distinct id1.invoice_id, id1.genre_id, id1.genre_name,
id2.genre_id as genre_id2, id2.genre_name as genre_name2
from invoice_details id1 join invoice_details id2
on id1.invoice_id = id2.invoice_id 
and id1.genre_id < id2.genre_id),

-- Album pairs
diff_album as
(select distinct id1.invoice_id, id1.album_id, id1.title,
id2.album_id as album_id2, id2.title as title2
from invoice_details id1 join invoice_details id2
on id1.invoice_id = id2.invoice_id 
and id1.album_id < id2.album_id),

-- Artist pairs
diff_artist as
(select distinct id1.invoice_id, id1.artist_id, id1.artist_name,
id2.artist_id as artist_id2, id2.artist_name as artist_name2
from invoice_details id1 join invoice_details id2
on id1.invoice_id = id2.invoice_id 
and id1.artist_id < id2.artist_id)

select genre_name, genre_name2, count(distinct invoice_id) as pairs_count
from diff_genre group by genre_id, genre_id2, genre_name, genre_name2
order by pairs_count desc limit 5;

select title, title2, count(distinct invoice_id) as pairs_count
from diff_album group by album_id, album_id2, title, title2
order by pairs_count desc limit 5;

select artist_name, artist_name2, count(distinct invoice_id) as pairs_count
from diff_artist group by artist_id, artist_id2, artist_name, artist_name2
order by pairs_count desc limit 5;

-- Q5 Regional Market Analysis:
-- Do customer purchasing behaviors and churn rates vary across different geographic regions or store locations?
-- How might these correlate with local demographic or economic factors?
with cust_purchase_behav as
(select i.customer_id, i.invoice_date, i.invoice_id, i.billing_country,
count(il.invoice_line_id) as total_track, i.total as spending
from invoice i join invoice_line il
on i.invoice_id = il.invoice_id
group by i.customer_id, i.invoice_date, i.invoice_id, i.billing_country
order by customer_id),

cust_habits as
(select customer_id, billing_country, count(*)/(timestampdiff(month, min(invoice_date), max(invoice_date)) +1) as frequency,
round(avg(total_track),2) as avg_basket_size, round(avg(spending),2) as avg_amount
from cust_purchase_behav
group by customer_id, billing_country)

select billing_country, count(customer_id) as total_customers, round(avg(frequency),3) as country_freq,
round(avg(avg_basket_size),2) as country_basket, round(avg(avg_amount),2) as country_spent
from cust_habits
group by billing_country;

-- churn

with purchase_timeline as
(select customer_id, billing_country, date(invoice_date) as purchase_date,
lead(date(invoice_date)) over(partition by customer_id) as next_purchase_date,
date_add(date(invoice_date), interval 1 year) as churn_threshold,
min(date(invoice_date)) over(partition by customer_id order by invoice_date) as first_date,
max(date(invoice_date)) over() as table_last_purchase
from invoice),

first_churn as
(select customer_id, billing_country, min(churn_threshold) as first_churn_date
from purchase_timeline
where churn_threshold < coalesce(next_purchase_date,table_last_purchase)
group by customer_id, billing_country),

first_purchase as
(select customer_id, billing_country, min(first_date) as first_purchase_date
from purchase_timeline group by customer_id, billing_country),

years as
(select distinct year(invoice_date) as purchase_year from invoice),

active_cust as
(select y.purchase_year, fp.billing_country, count(distinct fp.customer_id) as active_customer
from years y join first_purchase fp
on fp.first_purchase_date< date(concat(y.purchase_year, '-01-01'))
left join first_churn fc on fp.customer_id = fc.customer_id and
fp.billing_country = fc.billing_country
where fc.first_churn_date is null or
fc.first_churn_date >= date(CONCAT(y.purchase_year, '-01-01'))
group by y.purchase_year, fp.billing_country
),

churn_cust_year as
(select year(first_churn_date) as churn_year, billing_country, count( distinct customer_id) as churned_customers
from first_churn
group by year(first_churn_date), billing_country)

select acy.purchase_year as year, acy.billing_country,
round((coalesce(ccy.churned_customers,0)*100/acy.active_customer),2) as churn_rate
from active_cust acy left join
churn_cust_year ccy on acy.purchase_year = ccy.churn_year and
acy.billing_country = ccy.billing_country
order by year, billing_country;

-- Q6 Customer Risk Profiling:
-- Based on customer profiles (age, gender, location, purchase history), 
-- which customer segments are more likely to churn or pose a higher risk of reduced spending?
-- What factors contribute to this risk?

with cust_purchase_behav as
(select i.customer_id, i.invoice_date, i.invoice_id, count(il.invoice_line_id) as total_track, i.total as spending,
min(i.invoice_date) over(partition by i.customer_id) as first_purchase
from invoice i join invoice_line il
on i.invoice_id = il.invoice_id
group by i.customer_id, i.invoice_date, i.invoice_id
order by customer_id),

behavior as
(select customer_id, count(*)/(timestampdiff(month, min(invoice_date), max(invoice_date)) +1) as frequency,
round(avg(total_track),2) as avg_basket_size, round(avg(spending),2) as avg_amount
from cust_purchase_behav
group by customer_id),

thresholds as 
(select avg(frequency) as avg_freq, avg(avg_amount) as avg_spend
from behavior),

segmented_customers as (
select b.customer_id, b.frequency, t.avg_spend,
case when b.frequency >= t.avg_freq then 'High Frequency'else 'Low Frequency'
end as freq_segment,
case when b.avg_amount >= t.avg_spend then 'High Value' else 'Low Value'
end as value_segment
from behavior b cross join thresholds t
),

purchase_timeline as
(select customer_id, billing_country, date(invoice_date) as purchase_date,
lead(date(invoice_date)) over(partition by customer_id) as next_purchase_date,
date_add(date(invoice_date), interval 1 year) as churn_threshold,
min(date(invoice_date)) over(partition by customer_id order by invoice_date) as first_date,
max(date(invoice_date)) over() as table_last_purchase
from invoice),

first_churn as
(select customer_id, billing_country, min(churn_threshold) as first_churn_date
from purchase_timeline
where churn_threshold < coalesce(next_purchase_date,table_last_purchase)
group by customer_id, billing_country),

churn_flag as
(select customer_id, 1 as churned from first_churn),

churn_table as
(select sc.customer_id, sc.freq_segment, sc.value_segment,
coalesce(cf.churned,0) as churned
from segmented_customers sc left join churn_flag cf
on sc.customer_id = cf.customer_id),

-- comment out this query and remove the "," after churn_table cte to run this

-- select freq_segment, value_segment, count(*) as total_customers, sum(churned) as churned_customers
-- from churn_table
-- group by freq_segment, value_segment;

-- Q7 Customer Lifetime Value Modeling

-- to run this query undo changes done above
customer_clv as
(select customer_id, min(invoice_date) as first_purchase, max(invoice_date) as last_purchase,
(timestampdiff(month, min(invoice_date), max(invoice_date)) +1) as tenure,
count(*)/(timestampdiff(month, min(invoice_date), max(invoice_date)) +1) as frequency,
round(avg(total_track),2) as avg_basket_size, round(avg(spending),2) as avg_amount, sum(spending) as clv
from cust_purchase_behav
group by customer_id),

segment_clv as
(select cv.customer_id, cv.tenure, cv.frequency, cv.avg_basket_size, cv.avg_amount, 
ct.freq_segment, ct.value_segment, ct.churned, cv.clv
from customer_clv cv join churn_table ct
on cv.customer_id = ct.customer_id)

select churned, case when churned=1 then "churn" else "active" end as churn_segment, avg(clv) as avg_clv
from segment_clv group by churned;

select freq_segment, avg(clv) as avg_clv
from segment_clv group by freq_segment;

-- Q10 How can you alter the "Albums" table to add a new column named 
-- "ReleaseYear" of type INTEGER to store the release year of each album?
alter table album
add ReleaseYear int;

-- Q11
with cust_country_behav as
(select c.country, i.customer_id, sum(il.unit_price*il.quantity) as total_spent,
count(il.track_id) as tracks_purchased
from customer c join invoice i on c.customer_id = i.customer_id
join invoice_line il on i.invoice_id = il.invoice_id
group by c.country, i.customer_id)

select country, count(customer_id) as customer_count, round(avg(total_spent),2) as cust_avg_amount,
round(avg(tracks_purchased),2) as cust_avg_basket
from cust_country_behav
group by country;

-- Q9
-- finding some extra results for recommendations

select mt.media_type_id, mt.name as media_name, sum(il.quantity*il.unit_price) as revenue
from invoice_line il join track t on il.track_id = t.track_id
join media_type mt on t.media_type_id = mt.media_type_id
group by mt.media_type_id, mt.name
order by revenue desc;
