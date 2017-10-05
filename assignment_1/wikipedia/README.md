# Assignment 1: Wikipedia
Gauging how popular a programming language is important for companies judging whether or not they should adopt an emerging programming language. For that reason, industry analyst firm RedMonk has bi-annually computed a ranking of programming language popularity using a variety of data sources, typically from websites like GitHub and StackOverflow. See their top-20 ranking for June 2016 as an example.

In this assignment, we'll use our full-text data from Wikipedia to produce a rudimentary metric of how popular a programming language is, in an effort to see if our Wikipedia-based rankings bear any relation to the popular Red Monk rankings.

# How to run the job:

First you will have to download the wikipedia data (133M) and save it under `src/main/resources/wikipedia/wikipedia.dat`. To do that run the following command from `assignment_1/wikipedia`:

```
wget http://alaska.epfl.ch/~dockermoocs/bigdata/wikipedia.dat -P src/main/resources/wikipedia/wikipedia.dat
```

Then you can run the spark job using sbt:

```
sbt run
```

To run the tests:

```
sbt test
```
