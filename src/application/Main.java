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
// Main.java: main file for VGP

package application;

import core.Edge;
import input.Input;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Collections;
import java.util.List;
import output.Output;
import output.Statistics;
import partitioner.Partitioner;
import partitioner.coordinated_state.CoordinatedPartitionState;

public class Main {
    
    public static void main(String[] args) {        
        System.out.println("\n--------------------------------------------------");
        System.out.println(" VGP: A Software Package for one-pass Vertex-cut balanced Graph Partitioning.");
        System.out.println(" author: Fabio Petroni (http://www.fabiopetroni.com)");
        System.out.println("--------------------------------------------------\n");
        Globals GLOBALS = new Globals(args);
        System.out.println(" Parameters:\n");
        GLOBALS.print();
        Statistics stat = new Statistics(GLOBALS);
        System.out.print("\n Loading graph into main memory... ");
        Input input = new Input(GLOBALS);
        List<Edge> x = input.getDataset();
        System.out.print("\n Running program... ");
        start(GLOBALS,stat,input,x);
    }
    
    private static void start(Globals GLOBALS,Statistics stat, Input input, List<Edge> x){
        long begin_time = System.currentTimeMillis();
        List<Edge> dataset = x;
        Collections.shuffle(dataset);
        if (GLOBALS.PARTITION_STRATEGY.equalsIgnoreCase("grid") && !Partitioner.is_grid_compatible(GLOBALS.P)){
            System.out.println("\nError: Num partitions "+GLOBALS.P+" cannot be used for grid ingress.\n\n");
            System.exit((-1));
        }
        if (GLOBALS.PARTITION_STRATEGY.equalsIgnoreCase("pds") && !Partitioner.is_pds_compatible(GLOBALS.P)){
            System.out.println("\nError: Num partitions "+GLOBALS.P+" cannot be used for pds ingress.\n\n");
            System.exit((-1));
        }
        Partitioner p = new Partitioner(dataset,GLOBALS);
        CoordinatedPartitionState state  = p.performCoordinatedPartition();
        int [] load = state.getMachines_load();
        stat.computeReplicationFactor(state);  
        stat.computeStdDevLoad(load);
        double RF = round(stat.getReplicationFactor(),GLOBALS.PLACES);
        double std_dev = round(stat.getStdDevLoad(),GLOBALS.PLACES);  
        int [] load_edges = state.getMachines_load();
        int [] load_vertices = state.getMachines_loadVertices();
        int MAX_LOAD_EDGES = findMax(load_edges);
        int MAX_LOAD_VERTICES = findMax(load_vertices);
        long end_time = System.currentTimeMillis();
        long time = end_time-begin_time;
        time /= 1000; //sec
        System.out.println((int) time +" seconds");        
        System.out.println("\n Results:\n");
        System.out.println("\tReplication factor: "+RF);
        System.out.println("\tLoad relative standard deviation: "+std_dev);
        System.out.println("\tMax partition size (edge cardinality): "+MAX_LOAD_EDGES);
        System.out.println("\tMax partition size (vertex cardinality): "+MAX_LOAD_VERTICES);
        System.out.println("\n");     
        //WRITE OUTPUT ON FILE
        if (GLOBALS.OUTPUT_FILE_NAME!=null){
            Output.writeInfo(GLOBALS, RF, std_dev, MAX_LOAD_EDGES, MAX_LOAD_VERTICES);
            Output.writeVertexReplicas(GLOBALS, state);
        }
    }
    
    public static int findMax(int [] array){
        int MAX = -1;
        for (int i =0; i<array.length; i++){
            if (array[i]>MAX){
                MAX = array[i];
            }
        }
        return MAX;
    }
    
    public static double round(double value, int places) {
        if (places < 0) throw new IllegalArgumentException();
        BigDecimal bd = new BigDecimal(value);
        bd = bd.setScale(places, RoundingMode.HALF_UP);
        return bd.doubleValue();
    }
}
