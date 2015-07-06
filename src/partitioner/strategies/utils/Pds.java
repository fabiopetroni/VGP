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
// Pds.java: util class for the pds partitioning algorithm

package partitioner.strategies.utils;

import java.util.LinkedList;

public class Pds {
    
    public Pds(){}
    
    public LinkedList<Integer> get_pds(int p) {
        LinkedList<Integer> result = find_pds(p);
        // verify pdsness
        int pdslength = p*p + p + 1;
        int[] count = new int[pdslength];
        for (int i = 0; i<count.length; i++ ){ count[i] = 0;}
        for (int i = 0;i < result.size(); ++i) {
            for (int j = 0;j < result.size(); ++j) {
                if (i == j){ continue;}
                count[(result.get(i) - result.get(j) + pdslength) % pdslength]++;
            }
        }
        boolean ispds = true;
        for (int i = 1;i < count.length; ++i) {
            if (count[i] != 1) ispds = false;
        }

        // If success, return the result, else, return empty vector.
        if (ispds) {
            return result;
        } else {
            System.out.println("Fail to generate pds for p = "+p);
            return null;
        }
    }
       
     private LinkedList<Integer> find_pds(int p){
        LinkedList<Integer> result = new LinkedList<Integer>();
        for (int a = 0; a < p; ++a) {
            for (int b = 0; b < p; ++b) {
                if (b == 0 && a == 0){ continue;}
                for (int c = 1; c < p; ++c) {
                    if (test_seq(a,b,c,p,result)) {
                        return result;
                    }
                }
            }
        } 
        return result;
     }
     
     private boolean test_seq(int a, int b, int c, int p, LinkedList<Integer> result) {
        LinkedList<Integer> seq = new LinkedList<Integer>();
        int pdslength = p*p + p + 1;
        seq.add(0,0);
        seq.add(1,0);
        seq.add(2,1);
        int size = pdslength + 3;
        int ctr = 2;
        for (int i = 3; i < size; ++i) {
            int x = a * seq.get(i - 1) + b * seq.get(i - 2) + c * seq.get(i - 3);
            seq.add(i, x % p);
            if (seq.get(i) == 0){ctr++;}
            // PDS must be of length p + 1
            // and are the 0's of seq.
            if (i < pdslength && ctr > p + 1){ return false;}
        }
        if (seq.get(pdslength) == 0 && seq.get(pdslength + 1) == 0){ 
            // we are good to go
            // now find the 0s
            for (int i = 0; i < pdslength; ++i) {
                if (seq.get(i) == 0) { result.add(i); }
            }
            // probably not necessary. but verify that the result has length p + 1
            if (result.size() != p + 1) {
                result.clear();
                return false;
            }
            return true;
         }
         else {
            return false;
        } 
    }
}
