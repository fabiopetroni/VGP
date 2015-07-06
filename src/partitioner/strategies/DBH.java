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
// DBH.java: class implementing the DBH partitioning algorithm

package partitioner.strategies;

import core.Edge;
import java.util.Random;
import partitioner.PartitionState;
import partitioner.PartitionStrategy;
import partitioner.Record;
import partitioner.coordinated_state.CoordinatedPartitionState;
import application.Globals;

public class DBH implements PartitionStrategy{
    public static final int MAX_SHRINK = 100;
    double seed;
    int shrink;
    private Globals GLOBALS;
    
    public DBH(Globals G) {
        seed = Math.random();
        Random r = new Random(); 
        shrink = r.nextInt(MAX_SHRINK);
        this.GLOBALS = G;
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
        
        int machine_id = -1; 
        
        int shard_u = Math.abs((int) ( (int) u*seed*shrink) % P);  
        int shard_v = Math.abs((int) ( (int) v*seed*shrink) % P);  
        
        int degree_u = u_record.getDegree() +1;
        int degree_v = v_record.getDegree() +1;
        
        if (degree_v<degree_u){
            machine_id = shard_v;
        }
        else if (degree_u<degree_v){
            machine_id = shard_u;
        }
        else{ //RANDOM CHOICE
            //*** PICK A RANDOM ELEMENT FROM CANDIDATES
            Random r = new Random(); 
            int choice = r.nextInt(2);
            if (choice == 0){
                machine_id = shard_u;
            }
            else if (choice == 1){
                machine_id = shard_v;
            }
            else{
                System.out.println("ERROR IN RANDOM CHOICE DBH");
                System.exit(-1);
            }
        }
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
        
        //3-UPDATE DEGREES
        u_record.incrementDegree();
        v_record.incrementDegree();
        
        //*** RELEASE LOCK
        u_record.releaseLock();
        v_record.releaseLock();
    }
}