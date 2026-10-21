# Criteria 1
Dataset must have multiple unique entities. These are logical entities as opposed to how the raw tables are layed out.  
Min 5.

### How It's Met
1. character  
2. genre  
3. movie  
4. movie_genre (associative entity)  
5. external_ids (associative entity)  
6. person_info  
7. snack  
8. theater
9. actor
10. crew


# Criteria 2
Dataset must come from multiple sources of data.    
Min 4.

### How It's Met
IMDb, tmdb (TheMovieDB), AMC, GitHub.

# Criteria  3
Functional dependencies must hold on all tables, which means that the values are consistent across each record.  


### How It's Met
If our table has values for "city", "state", and "country", then the values of "city" and "state" together uniquely determine the value of "country". Example, our table `theater (GitHub)`.


# Criteria  4
There exists a column among the raw tables that stores more than one property in a given cell.  
Min 2.

### How It's Met
In table `theater (GitHub)`, column `location` stores the properties longitude and latitude in degrees. 
In table `person_info (tmdb)`, column `birthday` stores the properties year, month, and day of birth.
In table `person_info (tmdb)`, column `deathday` stores the properties year, month, and day of death.


# Criteria  5
There exists two raw tables coming from two different sources that represent the same entity.  
However, the entity may have slightly different properties in one table from another.  
Min 1.

### How It's Met
Table `movie (tmdb)` comes from TheMovieDB, while the table `movie IMDb)` comes from IMDb. Both represent the same entity `movie`, and have slightly different properties. For example, table `movie (IMDb)` has the properties `duration`, `cinematography`, `colour_film`, etc.

# Criteria  6
There exists a raw table that represents more than one entity.   
Min 1.

### How It's Met
Table `movie (tmdb)` represents the entities `genre` and `movie`.  
Table `crew (tmdb)` represents the entities `person_info` and `movie`.

# Criteria  7
There exists at least two disjoint entities coming from different sources that could be connected through a third entity.  
The third entity is not present in the dataset.  
Min 1.

### How It's Met
The entities `theater` and `snack` are disjoint and come from different source (GitHub and AMC, repsectively).  
A new entity `theater_showtimes` can connect `theater` to `movie`; and another new entity `concession service` can connect `snack` to `theatre`. Note: Due to the nature of our movies dataset, it was difficult to find disjoint entites that could be connected by a single third entity, but we could connect them through 2 "third" entities. The TAs confirmed this was ok to do.