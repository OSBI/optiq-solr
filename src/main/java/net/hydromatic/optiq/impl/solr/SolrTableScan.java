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

import net.hydromatic.optiq.rules.java.*;

import net.hydromatic.linq4j.expressions.*;

import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.RelWriter;
import org.eigenbase.rel.TableAccessRelBase;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;

import java.util.*;

/**
 * Relational expression representing a scan of a CSV file.
 *
 * <p>Like any table scan, it serves as a leaf node of a query tree.</p>
 */
public class SolrTableScan extends TableAccessRelBase implements EnumerableRel {
    final SolrTable solrTable;
    final int[] fields;

    protected SolrTableScan(RelOptCluster cluster, RelOptTable table,
                           SolrTable csvTable, int[] fields) {
        super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), table);
        this.solrTable = csvTable;
        this.fields = fields;

        assert csvTable != null;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        return new SolrTableScan(getCluster(), table, solrTable, fields);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("fields", Primitive.asList(fields));
    }

    @Override
    public RelDataType deriveRowType() {
        final RelDataTypeFactory.FieldInfoBuilder builder =
                getCluster().getTypeFactory().builder();
        for (int field : fields) {
            builder.add(table.getRowType().getFieldList().get(field));
        }
        return getCluster().getTypeFactory().createStructType(builder);
    }

    @Override
    public void register(RelOptPlanner planner) {
        planner.addRule(SolrPushProjectOntoTableRule.INSTANCE);
    }

    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getRowType(),
                        pref.preferArray());

        return implementor.result(
                physType,
                Blocks.toBlock(
                        Expressions.call(solrTable.getExpression(), "project",
                                Expressions.constant(fields))));
    }
}

// End SolrTableScan.java
