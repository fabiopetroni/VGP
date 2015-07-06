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
// Greedy.java: class implementing the Greedy partitioning algorithm

package partitioner.strategies;

import core.Edge;
import java.util.LinkedList;
import java.util.Random;
import partitioner.PartitionState;
import partitioner.PartitionStrategy;
import partitioner.Record;
import partitioner.coordinated_state.CoordinatedPartitionState;
import application.Globals;

public class Greedy implements PartitionStrategy{
    
    private Globals GLOBALS;
    
    public Greedy(Globals G){
        this.GLOBALS = G;
    }
    

    @Override
    public void performStep(Edge e, PartitionState state) {
        
        int P = GLOBALS.P;
        int epsilon = 1;
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
        
        int machine_id = -1; 
        
        //*** COMPUTE MAX AND MIN LOAD
        int MIN_LOAD = state.getMinLoad();
        int MAX_LOAD = state.getMaxLoad();
        
        //*** COMPUTE SCORES, FIND MIN SCORE, AND COMPUTE CANDIDATES PARITIONS
        LinkedList<Integer> candidates = new LinkedList<Integer>();
        double MAX_SCORE = 0;
        for (int m = 0; m<P; m++){
            int sd = 0;
            int td = 0;
            if (u_record.hasReplicaInPartition(m)){ sd = 1;}
            if (v_record.hasReplicaInPartition(m)){ td = 1;}
            int load = state.getMachineLoad(m);
            
            //OLD BALANCE
            double bal = (MAX_LOAD-load);
            bal /= (epsilon + MAX_LOAD - MIN_LOAD);
            if (bal<0){ bal = 0;}
            double SCORE_m = sd + td + bal;
            
            //NEW BALANCE
//            double bal = (MAX_LOAD-load);
//            bal /= (epsilon + MAX_LOAD);
//            if (bal<0){ bal = 0;}
//            double SCORE_m = sd + td + GLOBALS.ALPHA* bal;
            
            if (SCORE_m>MAX_SCORE){
                MAX_SCORE = SCORE_m;
                candidates.clear();
                candidates.add(m);
            }
            else if (SCORE_m==MAX_SCORE){
                candidates.add(m);
            }
        }   
        
        //*** CHECK TO AVOID ERRORS
        if (candidates.isEmpty()){
            System.out.println("ERRORE: GreedyObjectiveFunction.performStep -> candidates.isEmpty()");
            System.out.println("MAX_SCORE: "+MAX_SCORE);
            System.exit(-1);
        }
        
        //*** PICK A RANDOM ELEMENT FROM CANDIDATES
        Random r = new Random(); 
        int choice = r.nextInt(candidates.size());
        machine_id = candidates.get(choice);
        
        //1-UPDATE RECORDS
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
        
        //2-UPDATE EDGES
        state.incrementMachineLoad(machine_id,e);
        
        //*** RELEASE LOCK
        u_record.releaseLock();
        v_record.releaseLock();
    }
}
