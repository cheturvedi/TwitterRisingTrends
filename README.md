# TwitterRisingTrends
This project uses Apache Spark to stream live tweets as they are posted. 
The hashtags present in the tweets are extracted using regular expressions.
The top 10 hashtags with highest frequencies are reported for every second.

Furthermore, this project is capable of reporting most used hashtags in the past 5 minutes 
where frequencies were tallied in a rolling windows of 30 seconds.

