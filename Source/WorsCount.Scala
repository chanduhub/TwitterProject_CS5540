// hashtagsFile RDD Creation
val hashtagsFile = sc.textFile("C:/Users/chand/Documents/PB Project/hashtags.txt")

// hashtagsWords are words which are formed by flattening the each line from hashtagsFile 
val hashtagsWords = hashtagsFile.flatMap(line => line.split(" "))

// sums up occurrences by each word
val hashtagsCounts = hashtagsWords.map(word => (word, 1)).reduceByKey(_ + _)

// sorts words by count
val hashtagsSortedCount = hashtagsCounts.sortBy(-_._2)

//saves output to the file
hashtagsSortedCount.saveAsTextFile("C:/Users/chand/Documents/PB Project/hashtagsoutput")


val urlFile = sc.textFile("C:/Users/chand/Documents/PB Project/urls.txt")
val urlWords = urlFile.flatMap(line => line.split(" "))
val urlCounts = urlWords.map(word => (word, 1)).reduceByKey(_ + _)
val urlSortedCount = urlCounts.sortBy(-_._2)
urlSortedCount.saveAsTextFile("C:/Users/chand/Documents/PB Project/urlsoutput")
