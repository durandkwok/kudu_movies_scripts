# kudu_movies_scripts

Movies_with_the_highest_ratings script

Using Impala for Analytics and DML Operations in Kudu

Check the counts.  
Movie_info_kudu is a dimension table with 10,681 records. 
Ratings_kudu is a fact table with 10,000,054 records and 2,048,230 records with a rating of NULL. 

	•	select count(*) from ratings_kudu
	•	select count(*) from movie_info_kudu

List ratings and how many people voted

	•	select a.rating, count(userid) as voted
	from ratings_kudu a, movie_info_kudu b
	where 
	a.movieid = b.movieid
	group by a.rating
	order by rating desc


Let’s default their rating to 3 (average) then rerun previous query.

#Default rating to 3 (average) for people that didn’t vote.
	•	update ratings_kudu set rating = 3 where rating is null;


List genres related to Action movies with breakdown of all ratings and how many people voted

	•	select b.genres,a.rating, count(userid) as voted
	from ratings_kudu a, movie_info_kudu b
	where a.movieid = b.movieid
	and b.genres like 'Action%'
	group by b.genres, a.rating
	order by b.genres, rating desc


The query results above shows variations of action related movies.  
Let’s group all action related to just one genre call Action and rerun previous query. 
(Demonstrates change in dimension table)

# Update all Action related genres to just one category ‘Action’
	•	update movie_info_kudu set genres = 'Action'
	where genres like 'Action%';


List all Action movies with top ratings. 
	•	select b.genres, b.title, max(a.rating) as top_rating
	from ratings_kudu a, movie_info_kudu b
	where a.movieid = b.movieid
	and b.genres like 'Action%'
	group by b.genres,b.title
	order by b.genres, top_rating desc


Let’s backup the ‘Action’  ratings data set.  Demonstrate (CTAS)

	•	create table ratings_kudu_action
	distribute by hash (userid, movieid) into 3 buckets
	TBLPROPERTIES (
	'storage_handler'='com.cloudera.kudu.hive.KuduStorageHandler'
	,'kudu.table_name'='ratings_kudu_action'
	,'kudu.master_addresses'='<Kudu Master IP>:7051'
	,'kudu.key_columns'='userid, movieid'
	)
	as
	select a.* from
	ratings_kudu a, movie_info_kudu b
	where 
	a.movieid = b.movieid
	and b.genres like 'Action%';
