from pyspark import SparkContext
from pyspark.sql import SparkSession
from functools import reduce
from datetime import datetime
from pyspark.sql import SparkSession
from collections import defaultdict
import argparse
import time
import random
import findspark
import statistics
import sys
import os
import logging


def countTriangles2(colors_tuple, edges, rand_a, rand_b, p, num_colors):
    #We assume colors_tuple to be already sorted by increasing colors. Just transform in a list for simplicity
    colors = list(colors_tuple)  
    #Create a dictionary for adjacency list
    neighbors = defaultdict(set)
    #Creare a dictionary for storing node colors
    node_colors = dict()
    for edge in edges:

        u, v = edge
        node_colors[u]= ((rand_a*u+rand_b)%p)%num_colors
        node_colors[v]= ((rand_a*v+rand_b)%p)%num_colors
        neighbors[u].add(v)
        neighbors[v].add(u)

    # Initialize the triangle count to zero
    triangle_count = 0

    # Iterate over each vertex in the graph
    for v in neighbors:
        # Iterate over each pair of neighbors of v
        for u in neighbors[v]:
            if u > v:
                for w in neighbors[u]:
                    # If w is also a neighbor of v, then we have a triangle
                    if w > u and w in neighbors[v]:
                        # Sort colors by increasing values
                        triangle_colors = sorted((node_colors[u], node_colors[v], node_colors[w]))
                        # If triangle has the right colors, count it.
                        if colors==triangle_colors:
                            triangle_count += 1
    # Return the total number of triangles in the graph
    return triangle_count




def CountTriangles(partition_index, iterator):
    # Create a defaultdict to store the neighbors of each vertex
    neighbors = defaultdict(set)
    
    for edge in iterator:
            # Assume edge is an iterable with two elements
        u, v = edge

        if v is not None:
            neighbors[u].add(v)
            neighbors[v].add(u)

    # Initialize the triangle count to zero
    triangle_count = 0

    # Iterate over each vertex in the graph.
    # To avoid duplicates, we count a triangle <u, v, w> only if u<v<w
    for u in neighbors:
        # Iterate over each pair of neighbors of u
        for v in neighbors[u]:
            if v > u:
                for w in neighbors[v]:
                    # If w is also a neighbor of u, then we have a triangle
                    if w > v and w in neighbors[u]:
                        triangle_count += 1

    # Return the total number of triangles in the partition
    yield triangle_count

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



def MMR_ExactTC(rdd, C): 
    
    p = 8191
    a = random.randint(1, p-1)
    b = random.randint(0, p-1)
    
    def hash_function(u, C):
        h_c = ((a*u+b) % p) % C
        return h_c
    
    def add_mod_partition_index(partition_index, iterator):
        for value in iterator:
            for mod_value in range(C):
                yield tuple(sorted((mod_value, hash_function(value[0], C), hash_function(value[1], C)))), value


#-----------------------------------------------------ROUND 1----------------------------------------------------------------------------------

    result_rdd = rdd.mapPartitionsWithIndex(add_mod_partition_index)  #mapping 
    
    # Use groupByKey to group by key (x, y, z) -- intermediate phase
    grouped_rdd = result_rdd.groupByKey()

    
    
# ----------------------------------------------------ROUND 2-----------------------------------------------------------------------------------

    summed_rdd = grouped_rdd.map(lambda x: (x[0], countTriangles2(x[0], list(x[1]), a, b, p, C)))
    mapped_rdd = summed_rdd.map(lambda x: (1, x[1]))

    # Sum all the counts
    total_count = mapped_rdd.values().sum()

    # Return the total count
    return total_count


def main():
    # Initialize Spark
    findspark.init()
    spark = SparkSession.builder.appName("ExactTriangleCounting").getOrCreate()
    conf = spark.conf  # Added this line to fix the reference to conf
    conf.set("spark.locality.wait", "0s")
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    # Set the log level for the Python logger to ERROR
    logging.getLogger("py4j").setLevel(logging.ERROR)

    # Set the log level for the Python logger to ERROR
    logging.getLogger("py4j").setLevel(logging.ERROR)

    # Parse command line arguments
    try:
       C = int(sys.argv[1])
       R = int(sys.argv[2])
       F = int(sys.argv[3])
       input_path = sys.argv[4]
    except (ValueError, IndexError):
       print("Error: Please provide valid integer values for C, R, F, and a valid input path.")
       sys.exit(1)
    # Read the input graph into an RDD of edges
    rawData = sc.textFile(input_path)
    edges = rawData.map(lambda line: tuple(map(int, line.split(','))))

    # Cache the edges RDD for better performance
    edges = edges.partitionBy(32).cache()

    # Print information about the dataset
    print("Dataset =", input_path)
    print("Number of Edges =", edges.count())
    print("Number of Colors =", C)
    print("Number of Repetitions =", R)

    if F == 0:
        # Approximation algorithm with node coloring
        print("Approximation algorithm with node coloring")
        all_runtimes = []
        all_results = []
        for i in range(R):
            start_time = datetime.now()
            result = MR_ApproxTCwithNodeColors(edges, C)
            end_time = datetime.now()
            elapsed_time = (end_time - start_time).total_seconds() * 1000  # in milliseconds
            all_results.append(result)
            all_runtimes.append(elapsed_time)
        # Calculate and print the median and average running time
        median_result = sorted(all_results)[R // 2] if R % 2 == 1 else sum(sorted(all_results)[R // 2 - 1:R // 2 + 1]) / 2
        average_runtime = sum(all_runtimes) / R
        print("- Number of triangles (median over {} runs) = {}".format(R, median_result))
        print("- Running time (average over {} runs) = {:.0f} ms".format(R, average_runtime))


    elif F == 1:
        # Exact algorithm with node coloring
        print("Exact algorithm with node coloring")
        all_runtimes = []
        all_results = []
        for i in range(R):
            start_time = datetime.now()
            result = MMR_ExactTC(edges, C) # Implement MR_ExactTC function
            end_time = datetime.now()
            elapsed_time = (end_time - start_time).total_seconds() * 1000  # in milliseconds
            all_results.append(result)
            all_runtimes.append(elapsed_time)
		
        # Calculate and print the average running time
        average_runtime = sum(all_runtimes) / R
        print("- Number of triangles = ", all_results[0])
        print("- Running time (average over {} runs) = {:.0f} ms".format(R, average_runtime))


    # Stop Spark
    spark.stop()

if __name__ == "__main__":
    main()