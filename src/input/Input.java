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
// Input.java: file to load the graph into main memory

package input;

import core.Edge;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;
import application.Globals;

public class Input {
    
    private final Globals GLOBALS;
    private List<Edge> dataset;
    private int vertices;
    private long edges;
    
    public Input(Globals G){
        this.GLOBALS = G;   
        edges = 0;
        vertices = 0;
        readDatasetFromFile();
    }
    
    private void readDatasetFromFile(){        
        long begin_time = System.currentTimeMillis();
        TreeSet<Integer> vertices_tree= new TreeSet<Integer>();
        TreeSet<Edge> edges_tree = new TreeSet<Edge>();
        HashMap<Integer,Integer> degree = new HashMap<Integer,Integer>();
        try {
            FileInputStream fis = new FileInputStream(new File(GLOBALS.INPUT_FILE_NAME));
            InputStreamReader isr = new InputStreamReader(fis);
            BufferedReader in = new BufferedReader(isr);
            String line;
            while((line = in.readLine())!=null){
                if (line.startsWith("#")){continue;} //skip comments
                String values[] = line.split("\t");
                int u = Integer.parseInt(values[0]);
                int v = Integer.parseInt(values[1]);
                if (u!=v){  //self connection not allowed
                    Edge t = new Edge(u,v);
                    if ( edges_tree.add(t) ){ edges++; }
                    //System.out.println(t);

                    if( vertices_tree.add(u)){ vertices++; }
                    if( vertices_tree.add(v) ){ vertices++; }
                    
                    //DEGREE STATISTICS
                    if (!degree.containsKey(u)){ degree.put(u, 0);}
                    if (!degree.containsKey(v)){ degree.put(v, 0);}                 
                    int old_degree_u  = degree.get(u);
                    int old_degree_v  = degree.get(v);
                    degree.put(u, old_degree_u+1);
                    degree.put(v, old_degree_v+1);
                }
            }         
            in.close();
        } catch (IOException ex) {
            System.out.println("\nError: Input.readDatasetFromFile.\n\n");
            ex.printStackTrace();
            System.exit(-1);
        }          
        
        //DEBUG
        int MIN_DEGREE = Integer.MAX_VALUE;
        int MAX_DEGREE = Integer.MIN_VALUE;
        for (int v : degree.keySet()){
            int d = degree.get(v);
            if (d>MAX_DEGREE){ MAX_DEGREE = d; }
            if (d<MIN_DEGREE){ MIN_DEGREE = d; }
        }
        long end_time = System.currentTimeMillis();
        long time = end_time-begin_time;
        time /= 1000; //sec
        System.out.println((int) time +" seconds");
        System.out.println("\n Info:\n");
        System.out.println("\tvertices: "+vertices);
        System.out.println("\tedges: "+edges);
        System.out.println("\tmin-degree: "+MIN_DEGREE);
        System.out.println("\tmax-degree: "+MAX_DEGREE);
        
        dataset = new ArrayList<Edge>(edges_tree);
        vertices_tree.clear();
        edges_tree.clear();
        degree.clear();
    }
    
    public List<Edge> getDataset(){
        return dataset;
    }

    public int getVertices() {
        return vertices;
    }

    public long getEdges() {
        return edges;
    }
}