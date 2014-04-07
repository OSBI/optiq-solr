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

import com.google.common.collect.ImmutableMap;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.impl.AbstractSchema;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrDocumentList;

import java.util.*;

/**
 * Schema mapped onto a directory of CSV files. Each table in the schema
 * is a CSV file in that directory.
 */
public class SolrSchema extends AbstractSchema {
    private final String host;
    private final String core;
    public SolrServer server;
    public SolrSchema(
            SchemaPlus parentSchema,
            String host,
            String core) {
        super(parentSchema, core);
        this.host = host;
        this.core = core;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

        server = new HttpSolrServer(host);


        final List<SolrFieldType> fieldTypes = new ArrayList<SolrFieldType>();
        SolrQuery query = new SolrQuery();
        query.setQuery( "*:*" );
        query.setFacetLimit(100000);
        query.setParam("rows","10000");

        SolrDocumentList res = null;
        try {
            res = server.query(query).getResults();
        } catch (SolrServerException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        assert res != null;

        final SolrTable table;

        table = new SolrTable(this, core, res, server);

        builder.put(core, table);

        return builder.build();
    }

}

// End SolrSchema.java
