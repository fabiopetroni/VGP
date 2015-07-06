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
// Statistics.java: class to collect stats

package output;

import partitioner.PartitionState;
import application.Globals;

public class Statistics {
     
    private final Globals GLOBALS;
    
    public Statistics(Globals G){
        this.GLOBALS = G;
    }
    
    double replication_factor;
    public void computeReplicationFactor(PartitionState state){
        int replicas = state.getTotalReplicas();
        int vertices = state.getNumVertices();
//        System.out.println("replicas: "+replicas);
//        System.out.println("vertices: "+vertices);
        replication_factor = replicas;
        replication_factor /= vertices;
    }
    
    public double getReplicationFactor(){
        return replication_factor;
    }
    
    //compute standard deviation of the load
    double std_dev_load;
    public void computeStdDevLoad(int[] machines_load){
        int num_machines = GLOBALS.P;
        int weight[] = new int[num_machines];
        double average_load = 0;
        for (int m = 0; m< machines_load.length; m++){
            weight[m] = machines_load[m];
            average_load += weight[m];
        }
        average_load /= num_machines;
        double num = 0;
        for (int m = 0; m< num_machines; m++){
            num += Math.pow(weight[m] - average_load, 2);
        }
        num/=num_machines;
        std_dev_load = Math.sqrt(num);
        std_dev_load /= average_load; 
    }
    public double getStdDevLoad(){
        return std_dev_load;
    }
}
