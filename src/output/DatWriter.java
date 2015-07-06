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
// DatWriter.java: object to easily write a simple file .dat

package output;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

public class DatWriter {
    
    private String FILE_NAME;
    private BufferedWriter bw;
    
    public DatWriter (String f){
        FILE_NAME = f;
        open();
    }
    
    private void open(){
        try{
            File file = new File(FILE_NAME);
            // if file doesnt exists, then create it
            if (!file.exists()) {
                file.createNewFile();
            }
            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            bw = new BufferedWriter(fw);
        }catch(Exception e){
            System.out.println("ERRORE DatWriter.open() "+FILE_NAME);
            e.printStackTrace();
            System.exit(-1);
        }
    } 
    
    public void write(String content){
        try{
            bw.write(content);
        }
        catch(Exception e){
            System.out.println("ERRORE DatWriter.write("+content+") "+FILE_NAME);
            e.printStackTrace();
            System.exit(-1);
        }
    }
    
    public void close(){
       try{
            bw.close();
        }
        catch(Exception e){
            System.out.println("ERRORE DatWriter.close() "+FILE_NAME);
            e.printStackTrace();
            System.exit(-1);
        } 
    }
    
}
