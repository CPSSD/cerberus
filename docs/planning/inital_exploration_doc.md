MapReduce Framework - What goes into making it?
====================

What is map reduce?
---------------------

MapReduce is a programming model and an associated implementation for processing and generating large data sets. Users specify a map function that processes a key/value pair to generate a set of intermediate key/value pairs, and a reduce function that merges all intermediate values associated with the same intermediate key.

Components:
---------------------

* Master that distributes tasks among workers

* Worker mapping procedure

* Worker reduce procedure

* Client library

* Command line tool. This command line tool would be used to manage the cluster.

* Status page for viewing the progress and tasks of the MapReduce (not necessary but would be nice)

Architecture:
---------------------

One master, many workers. We don’t have to worry about anything such as master election. Masters can just be set machines with different binaries than workers.

The results of mapping operations are stored by the workers who performed the mapping operation. The workers performing reduces get the results from these workers using remote procedure calls. The results of reduce operations are stored in some globally accessible location. 

To allow users to write map and reduce operations in rust, we would allow users to implement a shared library file according to our API. This would be compiled and distributed among workers.

Questions/Challenges: 
---------------------

* Completed reduce tasks are stored in a global file system. Would we make our own filesystem for this or would we use something that already exists? If so, what would we use?

* How much work is there here? Can the project last a full 8 months? With a team of 5 people, it seems like it would be easy to get everything done pretty quickly.

First Sprint/How to get started:
---------------------

* First week of the first sprint would probably be spent working out architecture and defining shared RPC api.

* Simple dumb master. Does not handle failure or distribute tasks in a smart way.

* Simple workers. 

* Likely would not tackle client API, just hard code a certain map reduce operation into the master. 

* Map/Reduces would probably not be written in rust, easier to do it in an interpreted language at first.

* Would only work locally, no global file system.

Stretch Goals
---------------------

* Global file system for large input data and storing the results of the reduce, Google’s MapReduce frame-work originally used GFS for this.

* TLS security module so that communication with the master server is encrypted.

* Authentication system so that only authenticated users can schedule MapReduce operations.

* First class monitoring and logging support, built in dashboard that will show the status of running MR operations and any errors
