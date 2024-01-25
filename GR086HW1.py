from pyspark import SparkContext
from CountTriangles import CountTriangles
from functools import reduce
import argparse
import time
import random
from functions import MR_ApproxTCwithNodeColors, MR_ApproxTCwithSparkPartitions
import findspark
import statistics
import sys
import os

# Initialize findspark
findspark.init()


def parse_args():
    parser = argparse.ArgumentParser(description="MR Approximate Triangle Counting with Spark")
    parser.add_argument("C", type=int, help="Number of partitions")
    parser.add_argument("R", type=int, help="Number of runs")
    parser.add_argument("input_path", type=str, help="Path to the input graph file")
    return parser.parse_args()


def parse_edge(line):
    vertices = map(int, line.strip().split(','))
    return tuple(vertices)


def run_MR_ApproxTCwithNodeColors(edges, C, R):
    results=[]
    for i in range(R):
        results.append(MR_ApproxTCwithNodeColors(edges, C))
    return results

def main():
    
    # Check if the correct number of command-line arguments is provided
    args = parse_args()
    C, R, input_path = args.C, args.R, args.input_path
    filename = os.path.basename(input_path)

    # Set up Spark configuration and context
    sc = SparkContext(appName="YourAppName")
    
    # Read input graph
    raw_edges_rdd = sc.textFile(input_path)
    
    edges_rdd = raw_edges_rdd.mapPartitions(lambda partition: map(parse_edge, partition))
    print('\n\n\n\n')
    # Print information about the input
    print(f"Dataset = {filename}")
    print(f"Number of edges = {edges_rdd.count()}")
    print(f"Number of Colors = {C}")
    print(f"Number of Repetitions = {R}")
    
    print("Approximation through node coloring")
    
    # Run MR_ApproxTCwithNodeColors R times and print results
    start_time = time.time()
    estimates = run_MR_ApproxTCwithNodeColors(edges_rdd, C, R)
    median_estimate = statistics.median(estimates)
    average_runtime = (time.time() - start_time) / R
	
  
    print(f"- Number of triangles (median over {R} runs) = {median_estimate}")
    print(f"- Running time (average over {R} runs) = {average_runtime} seconds")

    # Run MR_ApproxTCwithSparkPartitions and print results
    start_time = time.time()
    final_estimate = MR_ApproxTCwithSparkPartitions(edges_rdd, C)
    running_time = time.time() - start_time
    print("Approximation through Spark partitions")
    print(f"- Number of Triangles = {final_estimate}")
    print(f"- Running time = {running_time} seconds")

    # Stop Spark context
    sc.stop()

if __name__ == "__main__":
    main()
