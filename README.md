404pattern
==========

Requires
--------

Mongodb

# brew install mongodb

Hadoop

Building
--------
mvn clean install


Running
-------

hadoop jar target/error404-pattern-discovery-0.0.1-SNAPSHOT-job.jar <data/input/> <data/output/>


*Mongo*
# db.pattern404.remove({})
# db.pattern404.find({})
# db.pattern404.find({}, {pattern: 1})


