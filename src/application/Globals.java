// Copyright (C) 2015 Fabio Petroni
// Contact: http://www.fabiopetroni.com
//
// This file is part of VGP (a software package for one-pass Vertex-cut balanced Graph Partitioning).
//
// VGP is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// VGP is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with VGP.  If not, see <http://www.gnu.org/licenses/>.
//
// Based on the publication:
// - Fabio Petroni, Leonardo Querzoni, Giorgio Iacoboni, Khuzaima Daudjee and Shahin Kamali (2015): 
//   "HDRF: Efficient Stream-Based Partitioning for Power-Law Graphs".
//   CIKM, 2014.
//
// Globals.java: class with parameters and arguments for VGP

package application;

public class Globals {
    
    //CONSTANT
    public int SLEEP_LIMIT = 1024;
    public final static int PLACES = 4;
    
    //APPLICATION PARAMETERS
    //MANDATORY
    public String INPUT_FILE_NAME;
    public int P;  //number of partitions
    //OPTIONAL
    public String PARTITION_STRATEGY= "hdrf"; // "hdrf", "greedy", "hashing", "grid", "pds", "dbh"
    public double LAMBDA = 1;    
    public int THREADS = Runtime.getRuntime().availableProcessors();
    public String OUTPUT_FILE_NAME;
    
    public Globals(String[] args){
        parse_arguments(args);
    }
    
    private void parse_arguments(String[] args){
        try{
            INPUT_FILE_NAME = args[0];
            P = Integer.parseInt(args[1]);   
            for(int i=2; i < args.length; i+=2){
                if(args[i].equalsIgnoreCase("-lambda")){
                    LAMBDA = Double.parseDouble(args[i+1]);
                }
                else if(args[i].equalsIgnoreCase("-algorithm")){
                    PARTITION_STRATEGY =args[i+1];
                    if (PARTITION_STRATEGY.equalsIgnoreCase("greedy")){}
                    else if (PARTITION_STRATEGY.equalsIgnoreCase("hdrf")){}
                    else if (PARTITION_STRATEGY.equalsIgnoreCase("hashing")){}
                    else if (PARTITION_STRATEGY.equalsIgnoreCase("grid")){}
                    else if (PARTITION_STRATEGY.equalsIgnoreCase("pds")){}
                    else if (PARTITION_STRATEGY.equalsIgnoreCase("dbh")){}
                    else{
                        System.out.println("\nInvalid algorithm "+PARTITION_STRATEGY+". Aborting.");
                        System.out.println("Valid algorithms: hdrf, greedy, hashing, grid, pds, dbh.\n");
                        System.exit(-1);
                    }
                }
                else if(args[i].equalsIgnoreCase("-threads")){
                    THREADS = Integer.parseInt(args[i+1]);
                }
                else if(args[i].equalsIgnoreCase("-output")){
                    OUTPUT_FILE_NAME = args[i+1];
                }
                else throw new IllegalArgumentException();
            }
        } catch (Exception e){
            System.out.println("\nInvalid arguments ["+args.length+"]. Aborting.\n");
            System.out.println("Usage:\n VGP graphfile nparts [options]\n");
            System.out.println("Parameters:");
            System.out.println(" graphfile: the name of the file that stores the graph to be partitioned.");
            System.out.println(" nparts: the number of parts that the graph will be partitioned into. Maximum value 256.");
            System.out.println("\nOptions:");
            System.out.println(" -algorithm string");
            System.out.println("\t specifies the algorithm to be used (hdrf greedy hashing grid pds dbh). Default hdrf.");
            System.out.println(" -lambda double");
            System.out.println("\t specifies the lambda parameter for hdrf. Default 1.");
            System.out.println(" -threads integer");
            System.out.println("\t specifies the number of threads used by the application. Default all available processors.");
            System.out.println(" -output string");
            System.out.println("\t specifies the prefix for the name of the files where the output will be stored (files: prefix.info, prefix.edges and prefix.vertices).");
            System.out.println();
            System.exit(-1);
        }
    }
    
    public void print(){
        System.out.println("\tgraphfile: "+INPUT_FILE_NAME);
        System.out.println("\tparts: "+P);
        System.out.print("\talgorithm: "+PARTITION_STRATEGY);
        if (PARTITION_STRATEGY.equalsIgnoreCase("hdrf")){ System.out.println(" (lambda: "+LAMBDA+")"); }
        else System.out.println("");
        System.out.println("\tthreads: "+THREADS);
        if (OUTPUT_FILE_NAME!=null){ System.out.println("\toutput: "+OUTPUT_FILE_NAME); }
    }
}
