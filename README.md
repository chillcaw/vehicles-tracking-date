### Design
My Task 1 and Task 2 code is more of less exactly the same because of the use
or a asynchronous runtime with an executor. To run a bench for Task 1 set
THREADS=1 and for Task 2 set THREADS=>1.

For concurrency I have chosen to use coroutines within the asyncio runtime.
Although none of the functionality is asynchronous, coroutines in async runtimes
are far easier to work with and easier to maintain than a manual threadpool.

While seemingly overkill for this task, using an async runtime opens up a lot of
functionality for distributed processing or comparisons to network sources
without having to rewrite a lot of code.

### Concurrency Conclusion
The dataset provided isn't a good candidate for concurrent processing for the
following reasons:
- car_number is a bad distribution key because tracking data for each vehicle
isn't even. Some car_numbers have much more associated tracking data than
others
- month is a bad distribution key also, as there is not a great enough date
range in the dataset to make use of all compute resources.

Since car_number was the closest thing to a decent distribution key, I spawned
coroutines for datasets grouped by car_number. The multithreaded approach was
only twice as fast as the single threaded approach, I assume this is because
some car_number(s) had much more data than others and concurrency applications
will only run as fast as their slowest process. I've illustrated this below
with an example:

##### Input:
- Car 1 - Processing Time - 0.1s
- Car 2 - Processing Time - 0.1s
- Car 3 - Processing Time - 20s

##### Results:
- Single Thread Speed: ~20.2s
- Concurrent Speed: ~20s

## Tooling Discussion
For this task I make heavy use of pandas' joins and aggregations since I was
only allowed to use pip packages. Given more choice I would actually opt to
perform these aggregations with queries to a RDBMS, this would make this script
easier to write and maintain but would also decouple business logic.
Inorder to make this much more sophisticated, since the tracking data is a
perfect candidate for CDC / on-update aggregates, I would lean more towards a
DMS Lambda approach than a batch processing approach.
