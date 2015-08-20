\# The following are used in order to pass parameters into the ebis.elastic.search.poc

\# The class that reads these properties is called ConfigCommons.java which can be found under the

\# package gov.nasa.jpl.ebis.elasticsearch.dao.util

\# If you take a look at that class you'll notice the instance variable that correspond to these property

\# types.

\# 

\# Please remember that when you create this file to put it at the 

\# base of your source path, aka at the root of the program level.

\# so if your programs path is \foo00\foo02\foo03 make sure the file is in the foo00 folder

--

env=DEV

dbtype=mongo

\# mongodburi property will accept a uri string

\# E.g.

\# mongodb://username:password@hostname:portnumber

mongodburi=mongodb://localhost:27017

mongodbname=name_of_db_you_want_to_use

mongodbcollection=name_of_collection_you_want_to_use

user=optional

pass=optional

\# The following fields can accept multiple values
\# The first query.files property will be associated with the first of each following property
\# The second query.files property will be associated with the second of each, etc.
\# If a parameter is unnecessary, leave the right side blank, ex:
\# node.names = 

query.files = test.json

index.names = testindex

type.names = testtype

cluster.names = testcluster

node.names = testnode

server.names = localhost

\# Each flag attribute, if set to true, will cause its associated operation
\# to be performed on the current query file (in this case, test.json).

\# write.flag determines whether to write the query results to a json file
write.flag = true

\# update operation will make the destination match the query results, 
\# removing any previous data
update.flag = true

\# index operation will append the query results to the destination data
\# in this case, the flag is not set to true, so index will not be performed
index.flag = false