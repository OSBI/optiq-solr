/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.impl.solr;

import net.hydromatic.linq4j.Enumerator;
import org.apache.solr.common.SolrDocument;

import java.util.*;

/** Enumerator that reads from a CSV file. */
class SolrEnumerator implements Enumerator<Object> {
    private  Iterator<SolrDocument> reader = null;
    private int[] fields = new int[0];
    private List<String> fieldnames;
    private Object current;
    private SolrFieldType[] fieldTypes;
    public SolrEnumerator(Iterator<SolrDocument> document, SolrFieldType[] fieldTypes, List<String> fieldnames) {
        this(document, fieldTypes, identityList(fieldTypes.length), fieldnames);
        this.fieldTypes = fieldTypes;
    }

    public SolrEnumerator(Iterator<SolrDocument> document, SolrFieldType[] fieldTypes, int[] fields, List<String> fieldnames) {
        this.fields = fields;
        this.reader = document;
        this.fieldnames = fieldnames;
        this.fieldTypes = fieldTypes;

    }

    public Object current() {
        return current;
    }

    public boolean moveNext() {
        try {
            if(reader.hasNext()){
                final SolrDocument strings = reader.next();
                if (strings == null) {
                    current = null;
                    //reader.close();
                    return false;
                }


                current = convertRow(strings);
                return true;
            }
            else{
                current = null;
                //reader.close();
                return false;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }


    private int findPosition(String name){
        return fieldnames.indexOf(name);
    }

    private Object[] convertRow(SolrDocument strings) {

        final Object[] objects = new Object[fields.length];
        Collection<String> f1 = strings.getFieldNames();
        Map<String,Object> f2 = strings.getFieldValueMap();
        for (String entry : f1) {

            final Object string = f2.get(entry);


            objects[findPosition(entry)] = string;

        }


        // THIS IS A FUDGE for MONDRIAN-1610 and needs removing ASAP.

      for(int i = 0; i<objects.length;i++){
          if(objects[i]==null){
              SolrFieldType o = fieldTypes[i];
              String n = "";
              if(o!=null){
               n = o.name();
              }

              if(n==""){
                  objects[i] = "N/A";
              }
              else if(n=="STRING"){
                  objects[i] = "N/A";
              }
              else if(n=="FLOAT"){
                 /* float b= 3.6f;
                  objects[i] = b;*/
              }
              else if(n=="DATE"){
                  //objects[i] = new Date(0);
              }
              else if(n=="INTEGER"){
                 // objects[i] = 0;
              }
              else if(n=="BOOLEAN"){
                  //objects[i] = false;
              }

          }
      }
        return objects;
    }


    public void reset() {
        throw new UnsupportedOperationException();
    }

    public void close() {
    }

    /** Returns an array of integers {0, ..., n - 1}. */
    static int[] identityList(int n) {
        int[] integers = new int[n];
        for (int i = 0; i < n; i++) {
            integers[i] = i;
        }
        return integers;
    }

}


// End SolrEnumerator.java
