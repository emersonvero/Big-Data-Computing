from pyspark import SparkContext
from CountTriangles import CountTriangles
from functools import reduce
import random


def MR_ApproxTCwithNodeColors(rdd, num_of_partitions): 
    
    
    
    #--------------------------------------------------#ROUND 1----------------------------------------------------------------------


    #Map Phase
    num_partitions = num_of_partitions
    p = 8191
    a = random.randint(1, p-1)
    b = random.randint(0, p-1)

    def hash_function(u, num_of_partitions=num_partitions):
        h_c = ((a*u+b) % p) % num_of_partitions
        return h_c

    def hashing_edges(iter):
        for edge in iter:
            v1, v2 = edge
            # Apply hash function to each vertex and return the tuple (partition, edge)
            if hash_function(v1, num_partitions) == hash_function(v2, num_partitions):
                yield (a, edge)

    # Map vertices to partitions using your hash function
    mapped_edges_rdd = rdd.mapPartitions(hashing_edges) #Intermediate pairs
    partitioned_rdd = mapped_edges_rdd.partitionBy(num_partitions,lambda x:x).values() #shuffling in 4 partitions
    np=partitioned_rdd.getNumPartitions()


    #Reduce Phase
    result = partitioned_rdd.mapPartitionsWithIndex(CountTriangles)





    #--------------------------------------------------#ROUND 2----------------------------------------------------------------------


    result_sum = result.reduce(lambda x, y: x + y)

    # Print or use the result_sum as needed
    estimated_triangles = (num_of_partitions**2)*result_sum
    
    return estimated_triangles






def MR_ApproxTCwithSparkPartitions(rdd, num_of_partitions): 
    
    
    
    #--------------------------------------------------#ROUND 1----------------------------------------------------------------------
    

    # Map vertices to partitions using your hash function
    mapped_edges_rdd = rdd.repartition(num_of_partitions) #spark partitions
    np=mapped_edges_rdd.getNumPartitions()


    #Reduce Phase
    result = mapped_edges_rdd.mapPartitionsWithIndex(CountTriangles)


    #--------------------------------------------------#ROUND 2----------------------------------------------------------------------


    result_sum = result.reduce(lambda x, y: x + y)

    # Print or use the result_sum as needed
    estimated_triangles = (num_of_partitions**2)*result_sum
    
    return estimated_triangles


