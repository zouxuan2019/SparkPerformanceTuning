### For spark job performance tuning including
1. Executor
2. Memory - TBC
- Determining Memory Consumption
  - For a dataset, Create RDD put it into cache, look at the "Storage" page in the Web UI.
  - For a particular object, use SizeEstimator’s estimate method
- Tuning Data Structures
  - Use more array, primitive types, fastutil library
  - Avoid nested structures with a lot of small objects and pointers
  - Using numeric IDs or enumeration objects instead of strings for keys
  - Less than 32 GB of RAM, set JVM flag -XX:+UseCompressedOops to make pointers be four bytes instead of eight. You can add these options in spark-env.sh
- Serialized RDD Storage
  - Recommend using Kryo if you want to cache data in serialized form
- Garbage Collection Tuning
  -  The cost of garbage collection is proportional to the number of Java objects.
  -  so using data structures with fewer objects (e.g. an array of Ints instead of a LinkedList) greatly lowers this cost. 
  -  An even better method is to persist objects in serialized form, as described above: now there will be only one object (a byte array) per RDD partition. Before trying other techniques, the first thing to try if GC is a problem is to use serialized caching.
  -  Measuring the Impact of GC
     - This can be done by adding -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps to the Java options
     - Next time your Spark job is run, you will see messages printed in the worker’s logs each time a garbage collection occurs.
     - These logs will be on your cluster’s worker nodes (in the stdout files in their work directories), not on your driver program.
     - The goal of GC tuning in Spark is to ensure that only long-lived RDDs are stored in the Old generation and that the Young generation is sufficiently sized to store short-lived objects. 
     - Details refer to https://spark.apache.org/docs/2.4.4/tuning.html
     
  - Other Consideration
    - Level of Parallelism
    - Memory Usage of Reduce Tasks
    - Broadcasting Large Variables: tasks larger than about 20 KB are probably worth optimizing.
    - Data Locality
3. Driver - TBC