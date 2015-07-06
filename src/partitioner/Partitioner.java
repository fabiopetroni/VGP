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
// Partitioner.java: class that manage the partitioning procedure (multithread)

package partitioner;

import core.Edge;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import partitioner.coordinated_state.CoordinatedPartitionState;
import partitioner.strategies.Constrained;
import partitioner.strategies.DBH;
import partitioner.strategies.Greedy;
import partitioner.strategies.HDRF;
import partitioner.strategies.Hashing;
import application.Globals;

public class Partitioner {
    
    private List<Edge> dataset;
    private PartitionStrategy algorithm;
    private Globals GLOBALS;

    public Partitioner(List<Edge> dataset, Globals G) {
        this.GLOBALS = G;
        this.dataset = dataset;
        //"greedy", "hdrf", "hashing", "grid", "pds
        if (GLOBALS.PARTITION_STRATEGY.equalsIgnoreCase("greedy")){ algorithm = new Greedy(GLOBALS); }
        else if (GLOBALS.PARTITION_STRATEGY.equalsIgnoreCase("hdrf")){ algorithm = new HDRF(GLOBALS); }
        else if (GLOBALS.PARTITION_STRATEGY.equalsIgnoreCase("hashing")){ algorithm = new Hashing(GLOBALS); }
        else if (GLOBALS.PARTITION_STRATEGY.equalsIgnoreCase("grid")){ algorithm = new Constrained(GLOBALS); }
        else if (GLOBALS.PARTITION_STRATEGY.equalsIgnoreCase("pds")){ algorithm = new Constrained(GLOBALS); }
        else if (GLOBALS.PARTITION_STRATEGY.equalsIgnoreCase("dbh")){ algorithm = new DBH(GLOBALS); }
    }  
    
    public CoordinatedPartitionState performCoordinatedPartition(){
        return startCoordinated();
    }
    
    private CoordinatedPartitionState startCoordinated(){
        CoordinatedPartitionState state = new CoordinatedPartitionState(GLOBALS);
        int processors = GLOBALS.THREADS;
        ExecutorService executor=Executors.newFixedThreadPool(processors);
        int n = dataset.size();
        int subSize = n / processors + 1;
        for (int t = 0; t < processors; t++) {
            final int iStart = t * subSize;
            final int iEnd = Math.min((t + 1) * subSize, n);
            if (iEnd>=iStart){
                List<Edge> list= dataset.subList(iStart, iEnd);
                Runnable x = new PartitionerThread(list, state, algorithm, new LinkedList());
                executor.execute(x);
            }
        }
        try { 
            executor.shutdown();
            executor.awaitTermination(60, TimeUnit.DAYS);
        } catch (InterruptedException ex) {System.out.println("InterruptedException "+ex);ex.printStackTrace();}
        return state;
    }  
    
    public static boolean is_grid_compatible(int partitions) {
        int nrow, ncol;
        double approx_sqrt = Math.sqrt(partitions);
        nrow = (int) approx_sqrt;
        for (ncol = nrow; ncol <= nrow + 2; ++ncol) {
            if (ncol * nrow == partitions) {
                return true;
            }
        }
        return false;
    }
    
    public static boolean is_pds_compatible(int partitions) {
        int p = (int) Math.sqrt(partitions-1);
        return (p>0 && ((p*p+p+1) == partitions));
    }
}
