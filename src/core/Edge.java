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
// Edge.java: file defining the Edge object

package core;

import java.io.Serializable;

public class Edge implements Comparable,Serializable{
    private final int u;
    private final int v;

    public Edge(int u,int v) {
        this.u = u;
        this.v = v;
    }    

    public int getU() {
        return u;
    }

    public int getV() {
        return v;
    }

    @Override
    public int hashCode() {
        String a = toString();
        int hash = a.hashCode();
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Edge other = (Edge) obj;
        if (this.u != other.u) {
            if ((this.u == other.v)&&(this.v == other.u)){
                return true;
            }
            else return false;
        }
        if (this.v != other.v) {
            if ((this.u == other.v)&&(this.v == other.u)){
                return true;
            }
            else return false;
        }
        return true;
    }
    
    @Override
    public String toString(){
        String s = "";
        if (u<v){
            s = u+","+v;
        }
        else{
            s = v+","+u;
        }
        return s;
    }

    @Override
    public int compareTo(Object obj) {
        if (obj == null) {
            System.out.println("ERROR: Edge.compareTo -> obj == null");
            System.exit(-1);
        }
        if (getClass() != obj.getClass()) {
            System.out.println("ERROR: Edge.compareTo -> getClass() != obj.getClass()");
            System.exit(-1);
        }
        final Edge other = (Edge) obj;
        return this.toString().compareTo(obj.toString()); //lexicographic order
    }
}
