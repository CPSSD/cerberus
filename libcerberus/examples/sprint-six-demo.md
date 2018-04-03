# Sprint 6 Demo Payload

The sprint 6 demo involves processing a dataset of user movie ratings to 
produce an aggregate rating for each movie. We then do two additional map reduces
on the output to produce top rated movie per genre and to group movie ratings by 
year released so that they can easily be found.

## Required files

Download the payload of movie lens rating data from:
https://drive.google.com/open?id=1HykgOTN_rVM6f_YIPIqM7FmxpD-2Jjqb

## Rating Aggregator

### Map

The map phase processes lines from two different csvs, one of movie details 
and one of movie ratings. It outputs each rating for a given movie id, as well
as the title and genre information for that movie id.

### Reduce

The reduce phase sums together all the float values produced by the map phase 
and outputs a csv formatted line with the movie title, genre and rating.

## Rating By Genre

### Map 

The map phase processes the output from the rating aggregator. It only handles the csv 
output lines, it doesn't use the output keys. For each genre, it outputs a movie - rating 
pair.

### Reduce

The reduce phase processes each movie - rating pair for a genre and outputs the movie 
with the highest rating.

## Rating By Year

### Map 

The map phase processes the output from the rating aggregator. It only handles the csv 
output lines, it doesn't use the output keys. It outputs movie ratings by movie title. 

### Parition 

To sort the output by year, we use a custom partitioning function to partition movies by
year released. The year released is extracted from the movie title.

### Reduce

The reduce is just an identity function that outputs the input intermediate data. 
