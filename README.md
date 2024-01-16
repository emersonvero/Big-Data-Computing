# Big Data 2022/23 Homeworks
Homeworks for the Big Data Course 2022/23, Computer Engineering Master's Degree @ Unipd

  * [Organization of the Repository](#organization-of-the-repository)
  * [Homework 1 - Triangle Counting](#homework-1---triangle-counting)
  * [Homework 2 - Triangle Counting in CloudVeneto Cluster](#homework-2---triangle-counting-in-cloudveneto-cluster)
  * [Homework 3 - Count Sketch with Spark Streaming](#homework-3---count-sketch-with-spark-streaming)

## Organization of the Repository
* `hw1`: datasets, output and code for the first homework
* `hw2`: datasets, output and code for the second homework
* `hw3`: datasets, output and code for the third homework

## Homework 1 - Triangle Counting
In the homework you must implement and test in Spark two MapReduce algorithms to count the number of distinct triangles in an undirected graph $G=(V,E)$, where a triangle is defined by 3 vertices $u,v,w\in V$, such that $(u,v),(v,w),(w,u)\in E$. 

![image](https://github.com/enricobolzonello/bigdata_homeworks/assets/73855124/54e673b1-a94b-4f7a-93a2-df0c9fda2cc4)

Both algorithms use an integer parameter $C\ge 1$, which is used to partition the data.

### ALGORITHM 1: 
Define a hash function ℎ𝐶 which maps each vertex 𝑢 in 𝑉 into a color $h_C(u)$ in $[0,C-1]$. To this purpose, we advise you to use the hash function
$$h_C(u)=(((a\cdot u+b)mod\: p)mod C)$$
where $p=8191$ (which is prime), $a$ is a random integer in $[1,p-1]$, and $b$ is a random integer in $[0,p-1]$.

##### Round 1:
* Create $C$ subsets of edges, where, for $0\leq i < C$, the i-th subset, $E(i)$ consist of all edges $(u,v)\in E$ such that $h_C(u)=h_C(v)=i$. Note that if the two endpoints of an edge have different colors, the edge does not belong to any $E(i)$ and will be ignored by the algorithm.
* Compute the number $t(i)$ triangles formed by edges of $E(i)$, separately for each $0\leq i < C$.
##### Round 2: 
* Compute and return $t_{final}=C^2\sum_{0\leq i < C}t(i)$ as final estimate of the number of triangles in $G$.

In the homework you must develop an implementation of this algorithm as a method/function `MR_ApproxTCwithNodeColors`.

### ALGORITHM 2:
##### Round 1:
* Partition the edges at random into $C$ subsets $E(0),E(1),...,E(C-1)$. Note that, unlike the previous algorithm, now every edge ends up in some $E(i).
* Compute the number $t(i)$ of triangles formed by edges of $E(i)$, separately for each $0\le i < C$.
##### Round 2: 
* Compute and return $t_{final}=C^2\sum_{0\leq i < C}t(i)$ as final estimate of the number of triangles in G.

In the homework you must develop an implementation of this algorithm as a method/function `MR_ApproxTCwithSparkPartitions` that, in Round 1, uses the partitions provided by Spark, which you can access through method mapPartitions.

### DATA FORMAT. 
To implement the algorithms assume that the vertices (set $V$) are represented as 32-bit integers, and that the graph $G$ is given in input as the set of edges $E$ stored in a file. Each row of the file contains one edge stored as two integers (the edge's endpoints) separated by comma (','). Each edge of $E$ appears exactly once in the file and $E$ does not contain multiple copies of the same edge.

## Homework 2 - Triangle Counting in CloudVeneto Cluster
In this homework, you will run a Spark program on the CloudVeneto cluster. As for Homework 1, the objective is to estimate (approximately or exactly) the number of triangles in an undirected graph 𝐺=(𝑉,𝐸). More specifically, your program must implement two algorithms:

### ALGORITHM 1: 
The same as Algorithm 1 in Homework 1.

### ALGORITHM 2: 
A 2-round MapReduce algorithm which returns the exact number of triangles. The algorithm is based on node colors (as Algorithm 1) and works as follows:
Let $C\ge 1$ be the number of colors and let $h_C(.)$ bee the hash function that assigns a color to each node used in Algorithm 1.
##### Round 1
* For each edge $(u,v)\in E$ separately create $C$ key-value pairs $(k_i,(u,v))$ with $i=0,1,...,C-1$ where each key $k_i$ is a triplet containing the three colors $h_C(u),h_C(v),i$ sorted in non-decreasing order.
* For each key $k=(x,y,z)$ let $L_k$ be the list of values (i.e., edges) of intermediate pairs with key $k$. Compute the number $t_k$ of triangles formed by the edges of $L_k$ whose node colors, in sorted order, are $x,y,z$. Note that the edges of $L_k$ may form also triangles whose node colors are not the correct ones: e.g., $(x,y,y)$ with $y\ne z$.
##### Round 2.
Compute and output the sum of all $t_k$'s determined in Round 1. It is easy to see that every triangle in the graph 𝐺 is counted exactly once in the sum. You can assume that the total number of $t_k$'s is small, so that they can be garthered in a local structure. Alternatively, you can use some ready-made reduce method to do the sum. Both approaches are fine.

## Homework 3 - Count Sketch with Spark Streaming
You must write a program which receives in input the following 6 command-line arguments (in the given order):
* An integer _D_: the number of rows of the count sketch
* An integer _W_: the number of columns of the count sketch
* An integer _left_: the left endpoint of the interval of interest
* An integer _right_: the right endpoint of the interval of interest
* An integer _K_: the number of top frequent items of interest
* An integer _portExp_: the port number
The program must read the first (approximately) 10M items of the stream $\Sigma$ generated from the remote machine at port _portExp_ and compute the following statistics. Let _R_ denote the interval _[left,right]_ and let $\Sigma_R$ be the substream consisting of all items of $\Sigma$ belonging to $R$.

The program must compute:
* A $D\times W$ count sketch for $\Sigma_R$
* The exact frequencies of all distinct items of $\Sigma_R$
* The true second moment $F_2$ of |\Sigma_R|. To avoid large numbers, normalize $F_2$ by dividing it by $|\Sigma_R|^2$.
* The approximate second moment $\tilde{F}_2$ of $\Sigma_R$ using count sketch, also normalized by dividing it by $|\Sigma_R|^2$.
* The average relative error of the frequency estimates provided by the count sketch where the average is computed over the items of $u\in \Sigma_R$ whose true frequency is $f_u\ge \phi(K)$, where $\phi(K)$ is the $K$-th largest frequency of the items of $\Sigma_R$. Recall that if $\tilde{f}_u$ is the estimated frequency for $u$, the relative error of is $|f_u-\tilde{f}_u| / f_u$.
