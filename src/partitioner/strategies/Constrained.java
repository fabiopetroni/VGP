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
// Constrained.java: class implementing two constrained partitioning solutions (grid and pds)

package partitioner.strategies;

import core.Edge;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Random;
import partitioner.PartitionState;
import partitioner.PartitionStrategy;
import partitioner.Record;
import partitioner.coordinated_state.CoordinatedPartitionState;
import partitioner.strategies.utils.Pds;
import application.Globals;

public class Constrained implements PartitionStrategy{
    
    public static final int MAX_SHRINK = 100;
    double seed;
    int shrink;
    int partitions;
    int nrows, ncols;
    LinkedList<Integer>[] constraint_graph;
    private Globals GLOBALS;
    
    public Constrained(Globals G){
        this.seed = Math.random();
        Random r = new Random(); 
        shrink = r.nextInt(MAX_SHRINK);
        this.GLOBALS = G;
        this.partitions = this.GLOBALS.P;
        this.constraint_graph = new LinkedList[this.partitions];
        if (this.GLOBALS.PARTITION_STRATEGY.equalsIgnoreCase("grid")) {
            make_grid_constraint();
        } else if (this.GLOBALS.PARTITION_STRATEGY.equalsIgnoreCase("pds")) {
            make_pds_constraint();
        }
    }
    
    private void make_grid_constraint() {
        initializeRowColGrid();
        for (int i = 0; i < partitions; i++) {
            LinkedList<Integer> adjlist = new LinkedList<Integer>();
            // add self
            adjlist.add(i);
            // add the row of i
            int rowbegin = (i/ncols) * ncols;
            for (int j = rowbegin; j < rowbegin + ncols; ++j)
              if (i != j) adjlist.add(j);
            // add the col of i
            for (int j = i % ncols; j < partitions; j+=ncols){
                  if (i != j) adjlist.add(j);
            }
            Collections.sort(adjlist);
            constraint_graph[i]=adjlist;
        }
        //DEBUG
//        for (int i = 0; i < partitions; i++) {
//            System.out.print(i+" --> [ ");
//            for (int j: constraint_graph[i]){
//                System.out.print(j+" ");
//            }
//            System.out.println("]");
//        }
    }
    
    private void initializeRowColGrid() {
        double approx_sqrt = Math.sqrt(partitions);
        nrows = (int) approx_sqrt;
        for (ncols = nrows; ncols <= nrows + 2; ++ncols) {
            if (ncols * nrows == partitions) {
                return;
            }
        }
        System.out.println("ERRORE Num partitions "+partitions+" cannot be used for grid ingress.");
        System.exit(-1);
    }
    
    private void make_pds_constraint() {
        int p = initializeRowColPds();
        Pds pds_generator = new Pds();
        LinkedList<Integer> results = new LinkedList<Integer>();
        if (p == 1) {
            results.add(0);
            results.add(2);
        } else {
            results = pds_generator.get_pds(p);
        }
        for (int i = 0; i < partitions; i++) {
            LinkedList<Integer> adjlist = new LinkedList<Integer>();
            for (int j = 0; j < results.size(); j++) {
                adjlist.add( (results.get(j) + i) % partitions);
            }
            Collections.sort(adjlist);
            constraint_graph[i]=adjlist;
        }
//        //DEBUG
//        for (int i = 0; i < partitions; i++) {
//            System.out.print(i+" --> [ ");
//            for (int j: constraint_graph[i]){
//                System.out.print(j+" ");
//            }
//            System.out.println("]");
//        }
    }
    
    private int initializeRowColPds() {
        int p = (int) Math.sqrt(partitions-1);
        if (!(p>0 && ((p*p+p+1) == partitions))){
            System.out.println("ERRORE Num partitions "+partitions+" cannot be used for pds ingress.");
            System.exit(-1);
        }
        return p;
    }    
    
    @Override
    public void performStep(Edge e, PartitionState state) {
        int P = GLOBALS.P;
        int u = e.getU();
        int v = e.getV();
        
        Record u_record = state.getRecord(u);
        Record v_record = state.getRecord(v);
        
        //*** ASK FOR LOCK
        int sleep = 2; while (!u_record.getLock()){ try{ Thread.sleep(sleep); }catch(Exception ex){} sleep = (int) Math.pow(sleep, 2);}
        sleep = 2; while (!v_record.getLock()){ try{ Thread.sleep(sleep); }catch(Exception ex){} sleep = (int) Math.pow(sleep, 2); 
        if (sleep>GLOBALS.SLEEP_LIMIT){u_record.releaseLock(); performStep(e,state); return;} //TO AVOID DEADLOCK
        }
        //*** LOCK TAKEN
        
        int shard_u = Math.abs((int) ( (int) u*seed*shrink) % P);  
        int shard_v = Math.abs((int) ( (int) v*seed*shrink) % P);  
        
        LinkedList<Integer> costrained_set = (LinkedList<Integer>) constraint_graph[shard_u].clone();
        costrained_set.retainAll(constraint_graph[shard_v]);
        
        //CASE 1: GREEDY ASSIGNMENT
        LinkedList<Integer> candidates = new LinkedList<Integer>();
        int min_load = Integer.MAX_VALUE;
        for (int m : costrained_set){
            int load = state.getMachineLoad(m);
            if (load<min_load){
                candidates.clear();
                min_load = load;
                candidates.add(m);
            }
            if (load == min_load){
                candidates.add(m);
            }
        }
        //*** PICK A RANDOM ELEMENT FROM CANDIDATES
        Random r = new Random(); 
        int choice = r.nextInt(candidates.size());
        int machine_id = candidates.get(choice);      
        
        //CASE 2 : RANDOM ASSIGNMENT
//        Random r = new Random(); 
//        int choice = r.nextInt(costrained_set.size());
//        int machine_id = costrained_set.get(choice);
        
        //UPDATE EDGES
        state.incrementMachineLoad(machine_id,e);
        
        //UPDATE RECORDS
        if (state.getClass() == CoordinatedPartitionState.class){
            CoordinatedPartitionState cord_state = (CoordinatedPartitionState) state;
            //NEW UPDATE RECORDS RULE TO UPFDATE THE SIZE OF THE PARTITIONS EXPRESSED AS THE NUMBER OF VERTICES THEY CONTAINS
            if (!u_record.hasReplicaInPartition(machine_id)){ u_record.addPartition(machine_id); cord_state.incrementMachineLoadVertices(machine_id);}
            if (!v_record.hasReplicaInPartition(machine_id)){ v_record.addPartition(machine_id); cord_state.incrementMachineLoadVertices(machine_id);}
        }
        else{
            //1-UPDATE RECORDS
            if (!u_record.hasReplicaInPartition(machine_id)){ u_record.addPartition(machine_id);}
            if (!v_record.hasReplicaInPartition(machine_id)){ v_record.addPartition(machine_id);}
        }
          
        //*** RELEASE LOCK
        u_record.releaseLock();
        v_record.releaseLock();
    }
}
