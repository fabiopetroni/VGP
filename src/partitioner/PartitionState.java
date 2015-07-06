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
// PartitionState.java: interface defining a partition state

package partitioner;

import core.Edge;
import java.util.SortedSet;

public interface PartitionState {
    public Record getRecord(int x);
    public int getMachineLoad(int m);
    public void incrementMachineLoad(int m, Edge e);
    public int getMinLoad();
    public int getMaxLoad();
    public int[] getMachines_load();
    public int getTotalReplicas();
    public int getNumVertices();
    public SortedSet<Integer> getVertexIds();
}
