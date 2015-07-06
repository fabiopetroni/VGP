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
// CoordinatedRecord.java: class implementing the Record interface

package partitioner.coordinated_state;

import java.io.Serializable;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import partitioner.Record;

public class CoordinatedRecord implements Serializable,Record{
    
    private TreeSet<Byte> partitions;   
    private AtomicBoolean lock;
    private int degree;
    
    public CoordinatedRecord() {
        partitions = new TreeSet<Byte>();
        lock = new AtomicBoolean(true);
        degree = 0;
    }
    
    @Override
    public Iterator<Byte> getPartitions(){
        return partitions.iterator();
    }
    
    @Override
    public void addPartition(int m){
        if (m==-1){ System.out.println("ERRORE! record.addPartition(-1)"); System.exit(-1);}
        partitions.add( (byte) m);
    }
    
    public void addAll(TreeSet<Byte> tree){
        partitions.addAll(tree);
    }
    
    @Override
    public boolean hasReplicaInPartition(int m){
        return partitions.contains((byte) m);
    }
    
    @Override
    public synchronized boolean getLock(){
        return lock.compareAndSet(true, false);
    }
    
    @Override
    public synchronized boolean releaseLock(){
        return lock.compareAndSet(false, true);
    }
    
    @Override
    public int getReplicas(){
        return partitions.size();
    }

    @Override
    public int getDegree() {
        return degree;
    }

    @Override
    public void incrementDegree() {
        this.degree++;
    }
    
    public static TreeSet<Byte> intersection(CoordinatedRecord x, CoordinatedRecord y){
        TreeSet<Byte> result = (TreeSet<Byte>) x.partitions.clone();
        result.retainAll(y.partitions);
        return result;
    }    
    
}