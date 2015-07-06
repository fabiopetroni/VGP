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
// Output.java: class to write the output partitions (info and vertices placement)

package output;

import application.Globals;
import java.util.Iterator;
import java.util.SortedSet;
import partitioner.PartitionState;
import partitioner.Record;

public class Output {
    
    public static void writeInfo(Globals GLOBALS, double RF, double std_dev, int MAX_LOAD_EDGES, int MAX_LOAD_VERTICES){
        DatWriter out = new DatWriter(GLOBALS.OUTPUT_FILE_NAME+".info");
        out.write("graphfile: "+GLOBALS.INPUT_FILE_NAME+"\n");
        out.write("parts: "+GLOBALS.P+"\n");
        out.write("algorithm: "+GLOBALS.PARTITION_STRATEGY);
        if (GLOBALS.PARTITION_STRATEGY.equalsIgnoreCase("hdrf")){ out.write(" (lambda: "+GLOBALS.LAMBDA+")\n"); }
        else out.write("\n");
        out.write("\n");
        out.write("Replication factor: "+RF+"\n");
        out.write("Load relative standard deviation: "+std_dev+"\n");
        out.write("Max partition size (edge cardinality): "+MAX_LOAD_EDGES+"\n");
        out.write("Max partition size (vertex cardinality): "+MAX_LOAD_VERTICES+"\n");
        out.close();        
    }
    
    public static void writeVertexReplicas(Globals GLOBALS, PartitionState state){
        DatWriter out = new DatWriter(GLOBALS.OUTPUT_FILE_NAME+".vertices");
        SortedSet<Integer> vertex_ids = state.getVertexIds();
        for (int x : vertex_ids){
            out.write(x+":");
            Record record = state.getRecord(x);
            Iterator<Byte> partitions =  record.getPartitions();
            while (partitions.hasNext()){
                int y = (( partitions.next()  & 0xFF ));                
                out.write(" "+y);
            }
            out.write("\n");
        }
        out.close();
    }
}
