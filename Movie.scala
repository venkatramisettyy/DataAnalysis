// Load the movie data from the file

val moviesRDD = sc.textFile("/home/srinivas/Downloads/Movies-Analytics-in-Spark-and-Scala-master/Movielens/movies.dat")


// Extracting necessary fields from each line


val movieFieldsRDD = moviesRDD.map(line => {
  val fields = line.split("::")
  (fields(0).toInt, fields(1), fields(2))
})


// Count the number of movies


val movieCount = movieFieldsRDD.count()
println(s"Total number of movies: $movieCount")


// sample movies


val sampleMovies = movieFieldsRDD.take(5)
println("Sample movies:")
sampleMovies.foreach(println)


// movies per decade


val moviesPerDecade = movieFieldsRDD.map { case (_, title, _) =>
  val yearPattern = "\\((\\d{4})\\)".r
  val year = yearPattern.findFirstMatchIn(title).map(_.group(1)).getOrElse("Unknown")
  val decade = year.substring(0, 3) + "0s"
  (decade, 1)
}.reduceByKey(_ + _)

println("Number of movies per decade:")
moviesPerDecade.foreach(println)


// movies in year 1995


val moviesByYear = movieFieldsRDD.filter { case (_, title, _) =>
  val yearPattern = "\\((\\d{4})\\)".r
  val year = yearPattern.findFirstMatchIn(title).map(_.group(1)).getOrElse("Unknown")
  year == "1995" // Replace with the desired year
}
println(s"Movies released in 1995:")
moviesByYear.foreach(println)


// movie with longest title


val moviesWithLongestTitles = movieFieldsRDD.sortBy { case (_, title, _) =>
  -title.length
}.take(10)
println("Movies with the longest titles:")
moviesWithLongestTitles.foreach(println)


// movie in each genre


val genreCountsRDD = movieFieldsRDD.flatMap { case (_, _, genres) =>
  genres.split('|')
}.map(genre => (genre, 1))
val moviesPerGenre = genreCountsRDD.reduceByKey(_ + _)
println("Number of movies in each genre:")
moviesPerGenre.foreach(println)















// distict genres


val movies_rdd=sc.textFile("/home/srinivas/Downloads/Movies-Analytics-in-Spark-and-Scala-master/Movielens/movies.dat")
val genres=movies_rdd.map(lines=>lines.split("::")(2))
val testing=genres.flatMap(line=>line.split('|'))
val genres_distinct_sorted=testing.distinct().sortBy(_(0))


genres_distinct_sorted.saveAsTextFile("output/distict_genre-csv")



// latest movies


val movie_nm=movies_rdd.map(lines=>lines.split("::")(1))
val year=movie_nm.map(lines=>lines.substring(lines.lastIndexOf("(")+1,lines.lastIndexOf(")")))
val latest=year.max
val latest_movies=movie_nm.filter(lines=>lines.contains("("+latest+")")).saveAsTextFile("output/latest_movies")


// movie starting with letter or numbers


val movies=movies_rdd.map(lines=>lines.split("::")(1))
val string_flat=movies.map(lines=>lines.split(" ")(0))
// check for the first character for a letter then find the count
val movies_letter=string_flat.filter(word=>Character.isLetter(word.head)).map(word=>(word.head.toUpper,1))
val movies_letter_count=movies_letter.reduceByKey((k,v)=>k+v).sortByKey()
// check for the first character for a digit then find the count
val movies_digit=string_flat.filter(word=>Character.isDigit(word.head)).map(word=>(word.head,1))
val movies_digit_count=movies_digit.reduceByKey((k,v)=>k+v).sortByKey()
// Union the partitions into a same file
val result=movies_digit_count.union(movies_letter_count).repartition(1).saveAsTextFile("output/movie_ starting_with_letter_numbers-csv")


// top 10 most viewed movies


val ratingsRDD=sc.textFile("/home/srinivas/Downloads/Movies-Analytics-in-Spark-and-Scala-master/Movielens/ratings.dat")
val movies=ratingsRDD.map(line=>line.split("::")(1).toInt)
val movies_pair=movies.map(mv=>(mv,1))

val movies_count=movies_pair.reduceByKey((x,y)=>x+y)
val movies_sorted=movies_count.sortBy(x=>x._2,false,1)

val mv_top10List=movies_sorted.take(10).toList
val mv_top10RDD=sc.parallelize(mv_top10List)

val mv_names=sc.textFile("/home/srinivas/Downloads/Movies-Analytics-in-Spark-and-Scala-master/Movielens/movies.dat").map(line=>(line.split("::")(0).toInt,line.split("::")(1)))



// movie in each genre


val genre=movies_rdd.map(lines=>lines.split("::")(2))
val flat_genre=genre.flatMap(lines=>lines.split("\\|"))
val genre_kv=flat_genre.map(k=>(k,1))
val genre_count=genre_kv.reduceByKey((k,v)=>(k+v))
val genre_sort= genre_count.sortByKey()
genre_sort.saveAsTextFile("output/movie_in_each_genre-csv")
















// // // users.dat


val usersRDD = spark.sparkContext.textFile("/home/srinivas/Downloads/Movies-Analytics-in-Spark-and-Scala-master/Movielens/users.dat")

// Transform the RDD to extract the gender field:
val genderRDD = usersRDD.map(line => line.split("::")(1))

//Counting the number of users in the dataset:

val userCount = usersRDD.count()
println(s"Number of users: $userCount")


// Count the occurrences of each gender

val genderCountsRDD = genderRDD.countByValue()

// Transforming the RDD to extract specific fields:

val userDataRDD = usersRDD.map(line => {
  val fields = line.split("::")
  (fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt, fields(4))
})

// Get the count of male and female genders:

val maleCount = genderCountsRDD.getOrElse("M", 0L)
val femaleCount = genderCountsRDD.getOrElse("F", 0L)

println(s"Total number of males: $maleCount")
println(s"Total number of females: $femaleCount")

// Find the gender with the highest count:

val (mostRatedGender, count) = genderCountsRDD.maxBy(_._2)
println(s"The gender with more ratings is $mostRatedGender with a count of $count")

// Find the gender with the lowest count:

val (leastRatedGender, count) = genderCountsRDD.minBy(_._2)
println(s"The gender with more ratings is $leastRatedGender with a count of $count")

// Calculate the average age of all users:

val totalAge = usersRDD.map(line => line.split("::")(2).toInt).sum()
val averageAge = totalAge.toDouble / userCount
println(f"Average age of users: $averageAge%.2f")


// Grouping users by occupation:

val usersByOccupationRDD = userDataRDD.groupBy { case (_, _, _, occupation, _) => occupation }

// Counting the number of users in each occupation:

val usersCountByOccupationRDD = usersByOccupationRDD.map { case (occupation, users) => (occupation, users.size) }

// Determine the most common occupation among users:

val occupationRDD = usersRDD.map(line => line.split("::")(3))
val occupationCountsRDD = occupationRDD.countByValue()
val mostCommonOccupation = occupationCountsRDD.maxBy(_._2)._1
val occupationCount = occupationCountsRDD(mostCommonOccupation)
println(s"The most common occupation is '$mostCommonOccupation' with a count of $occupationCount")

// Find the top 5 most frequent zip codes:

val zipCodeRDD = usersRDD.map(line => line.split("::")(4))
val zipCodeCountsRDD = zipCodeRDD.countByValue().toSeq.sortBy(-_._2).take(5)
println("Top 5 most frequent zip codes:")
zipCodeCountsRDD.foreach { case (zipCode, count) =>
  println(s"$zipCode: $count")
}





















// Transform the RDD to extract the gender and age fields for M gender:

val maleAgeRDD = usersRDD.filter(line => line.split("::")(1) == "M").map(line => {
  val fields = line.split("::")
  fields(2).toInt
})

// Calculate the total age and count of males:

val totalAge = maleAgeRDD.sum()
val maleCount = maleAgeRDD.count()

val averageAge = totalAge / maleCount.toDouble
println(s"The average age of M gender who rated is: $averageAge")

// Transform the RDD to extract the gender and age fields for F gender:

val femaleAgeRDD = usersRDD.filter(line => line.split("::")(1) == "F").map(line => {
  val fields = line.split("::")
  fields(2).toInt
})

// Calculate the total age and count of females:

val totalAge = femaleAgeRDD.sum()
val femaleCount = femaleAgeRDD.count()

val averageAge = totalAge / femaleCount.toDouble
println(s"The average age of F gender who rated is: $averageAge")



// // // rating.dat


// average rating for all movies


val ratingsData = sc.textFile("/home/srinivas/Downloads/Movies-Analytics-in-Spark-and-Scala-master/Movielens/ratings.dat")

// Split each line and extract the rating
val ratings = ratingsData.map(line => line.split("::")(2).toDouble)

// Map each rating to (movieId, rating) pair
val movieRatings = ratings.map(rating => (1, rating)) // Assuming all movies have ID 1

// Calculate the sum and count of ratings for each movie
val movieRatingSumCount = movieRatings.aggregateByKey((0.0, 0))(
  (acc, rating) => (acc._1 + rating, acc._2 + 1),
  (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
)

// Calculate the average rating for each movie
val movieAvgRatings = movieRatingSumCount.mapValues {
  case (ratingSum, ratingCount) => ratingSum / ratingCount
}

// Print the average rating for each movie
movieAvgRatings.foreach {
  case (movieId, avgRating) => println(s"Movie $movieId: Average Rating $avgRating")
}


// rating of movie 1

// Split each line and extract the movie ID and rating
val movieRatings = ratingsData.map(line => {
  val fields = line.split("::")
  val movieId = fields(1).toInt
  val rating = fields(2).toDouble
  (movieId, rating)
})

// Calculate the sum and count of ratings for each movie
val movieRatingSumCount = movieRatings.aggregateByKey((0.0, 0))(
  (acc, rating) => (acc._1 + rating, acc._2 + 1),
  (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
)

// Calculate the average rating for each movie
val movieAvgRatings = movieRatingSumCount.mapValues {
  case (ratingSum, ratingCount) => ratingSum / ratingCount
}

// Function to get the average rating for a given movie ID
def getAverageRating(movieId: Int): Option[Double] = {
  movieAvgRatings.lookup(movieId).headOption
}

// Example usage: Get the average rating for movie with ID 1
val movieId = 1
val averageRating = getAverageRating(movieId)

averageRating match {
  case Some(rating) => println(s"Average rating for Movie $movieId: $rating")
  case None => println(s"No rating found for Movie $movieId")
}

 
// top rating movies


// Calculate the average rating for each movie
val movieAvgRatings = movieRatingSumCount.mapValues {
  case (ratingSum, ratingCount) => ratingSum / ratingCount
}

// Get the top 10 movies with the highest average ratings
val topRatedMovies = movieAvgRatings.takeOrdered(10)(Ordering[Double].reverse.on(_._2))

// Display the top rated movies
println("Top 10 Movies with Highest Average Ratings:")
topRatedMovies.foreach {
  case (movieId, avgRating) => println(s"Movie ID: $movieId, Average Rating: $avgRating")
}












