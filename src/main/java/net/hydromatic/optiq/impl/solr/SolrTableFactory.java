package net.hydromatic.optiq.impl.solr;

import net.hydromatic.optiq.TableFactory;
import net.hydromatic.optiq.*;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrDocumentList;
import org.eigenbase.reltype.RelDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: bugg
 * Date: 16/09/13
 * Time: 21:49
 * To change this template use File | Settings | File Templates.
 */
@SuppressWarnings("UnusedDeclaration")
public class SolrTableFactory implements TableFactory<SolrTable> {
    // public constructor, per factory contract
    public SolrTableFactory() {
    }

    public SolrTable create(SchemaPlus schema, String name,
                            Map<String, Object> map, RelDataType rowType) {
        String host = (String) map.get("host");

        SolrServer server = new HttpSolrServer(host);

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

        final List<SolrFieldType> list = new ArrayList<SolrFieldType>();
        return new SolrTable(schema, name, res, server);
    }
}