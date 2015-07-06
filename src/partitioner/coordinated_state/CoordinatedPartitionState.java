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
// CoordinatedPartitionState.java: class implementing the PartitionState interface

package partitioner.coordinated_state;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import partitioner.PartitionState;
import application.Globals;
import core.Edge;
import java.util.SortedSet;
import java.util.TreeSet;
import output.DatWriter;

public class CoordinatedPartitionState implements PartitionState{
    private HashMap<Integer,CoordinatedRecord> record_map;
    private AtomicInteger[] machines_load_edges;
    private AtomicInteger[] machines_load_vertices;
    private final Globals GLOBALS; 
    int MAX_LOAD;
    DatWriter out; //to print the final partition of each edge

    public CoordinatedPartitionState(Globals G) {
        this.GLOBALS = G;
        record_map = new HashMap<Integer,CoordinatedRecord>();
        machines_load_edges = new AtomicInteger[GLOBALS.P];
        for (int i = 0; i<machines_load_edges.length;i++){ 
            machines_load_edges[i] = new AtomicInteger(0); 
        }        
        machines_load_vertices = new AtomicInteger[GLOBALS.P];
        for (int i = 0; i<machines_load_vertices.length;i++){ 
            machines_load_vertices[i] = new AtomicInteger(0); 
        }        
        MAX_LOAD = 0;
        if (GLOBALS.OUTPUT_FILE_NAME!=null){
            out = new DatWriter(GLOBALS.OUTPUT_FILE_NAME+".edges");
        }
    }
    
    public synchronized void incrementMachineLoadVertices(int m) {
        machines_load_vertices[m].incrementAndGet();
    }
    
    public int[] getMachines_loadVertices() {
        int [] result = new int[machines_load_vertices.length];
        for (int i = 0; i<machines_load_vertices.length;i++){ 
            result[i] = machines_load_vertices[i].get();
        }
        return result;
    }

    @Override
    public synchronized CoordinatedRecord getRecord(int x){
        if (!record_map.containsKey(x)){
            record_map.put(x, new CoordinatedRecord());
        }
        return record_map.get(x);
    }
    
    @Override
    public int getNumVertices(){
        return record_map.size();
    }
    
    @Override
     public int getTotalReplicas(){
        int result = 0;
        for (int x : record_map.keySet()){
            int r = record_map.get(x).getReplicas();
            if (r>0){
                result += record_map.get(x).getReplicas();
            }
            else{
                result++;
            }
        }
        return result;
    }

    @Override
    public synchronized int getMachineLoad(int m) {
        return machines_load_edges[m].get();
    }

    @Override
    public synchronized void incrementMachineLoad(int m, Edge e) {
        int new_value = machines_load_edges[m].incrementAndGet();
        if (new_value>MAX_LOAD){
            MAX_LOAD = new_value;
        }
        if (GLOBALS.OUTPUT_FILE_NAME!=null){
            out.write(e+": "+m+"\n");
        }
    }
    
    @Override
    public int[] getMachines_load() {
        int [] result = new int[machines_load_edges.length];
        for (int i = 0; i<machines_load_edges.length;i++){ 
            result[i] = machines_load_edges[i].get();
        }
        return result;
    }

    @Override
    public synchronized int getMinLoad() {
        int MIN_LOAD = Integer.MAX_VALUE;
        for (AtomicInteger load : machines_load_edges) {
            int loadi = load.get();
            if (loadi<MIN_LOAD){
                MIN_LOAD = loadi;
            }
        }
        return MIN_LOAD;
    }

    @Override
    public int getMaxLoad() {
        return MAX_LOAD;
    }

    @Override
    public SortedSet<Integer> getVertexIds() {
        if (GLOBALS.OUTPUT_FILE_NAME!=null){ out.close(); }        
        return new TreeSet<Integer>(record_map.keySet());
    }
    
}
