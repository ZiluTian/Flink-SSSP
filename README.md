# Flink-SSSP
The directory `/SSSP/*` contains a minimal example for defining an iterative algorithm in Flink 1.16.1 using Scala 2.12. Tested on CentOS 7 with Java 8 and 11. 

You package the application into a uber jar with `sbt assembly` and then submit the jar to a running Flink cluster: 

```
flink run -c Example.SSSP /path/to/your/project/target/scala-2.12/EXAMPLE-assembly-0.1-SNAPSHOT.jar
```

You should see the following output: 
```
(1,1.0)
(5,1.0)
(4,1.0)
(6,1.0)
(0,1.0)
(3,1.0)
(2,1.0)
(7,1.0)
```
