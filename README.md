# VGP
A software package for one-pass Vertex-cut balanced Graph Partitioning.

Based on the publication:

-F. Petroni, L. Querzoni, G. Iacoboni, K. Daudjee and S. Kamali: "Hdrf: Efficient stream-based partitioning for power-law graphs". CIKM, 2015.

If you use the application please cite the paper.

HDRF has been integrated in [GraphLab PowerGraph](https://github.com/dato-code/PowerGraph)!

###Usage:

```
VGP graphfile nparts [options]
```

Parameters:
- `graphfile`: the name of the file that stores the graph to be partitioned.
- `nparts`: the number of parts that the graph will be partitioned into. Maximum value 256.

Options:
- `-algorithm string`  ->  specifies the algorithm to be used (hdrf greedy hashing grid pds dbh). Default hdrf.
- `-lambda double`  ->  specifies the lambda parameter for hdrf. Default 1.
- `-threads integer`  ->  specifies the number of threads used by the application. Default all available processors.
- `-output string`  ->  specifies the prefix for the name of the files where the output will be stored (files: prefix.info, prefix.edges and prefix.vertices).


For a more in-depth discussion see the manual.

###Example

```
java -jar dist/VGP.jar example/sample_graph.txt 4 -algorithm hdrf -lambda 3 -threads 1 -output example/output  
```
