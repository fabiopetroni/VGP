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
// PartitionerThread.java: thread that perform the partitioning

package partitioner;

import core.Edge;
import java.util.LinkedList;
import java.util.List;

public class PartitionerThread implements Runnable{

    private final List<Edge> list;
    private final PartitionState state;
    private final PartitionStrategy algorithm;
    LinkedList<Integer> id_partitions;

    public PartitionerThread(List<Edge> list, PartitionState state, PartitionStrategy algorithm, LinkedList<Integer> ids) {
        this.list = list;
        this.state = state;
        this.algorithm = algorithm;
        this.id_partitions = ids;
    }
    
    @Override
    public void run() {
        for (Edge t: list){
            algorithm.performStep(t, state);
        }
    }
}
