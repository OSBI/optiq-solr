package net.hydromatic.optiq.impl.solr;

import org.eigenbase.rel.ProjectRel;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexNode;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: bugg
 * Date: 16/09/13
 * Time: 21:29
 * To change this template use File | Settings | File Templates.
 */
public class SolrPushProjectOntoTableRule extends RelOptRule {
    public static final SolrPushProjectOntoTableRule INSTANCE =
            new SolrPushProjectOntoTableRule();

    private SolrPushProjectOntoTableRule() {
        super(
                operand(ProjectRel.class,
                        operand(SolrTableScan.class, none())),
                "CsvPushProjectOntoTableRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final ProjectRel project = call.rel(0);
        final SolrTableScan scan = call.rel(1);
        int[] fields = getProjectFields(project.getProjects());
        if (fields == null) {
            // Project contains expressions more complex than just field references.
            return;
        }
        call.transformTo(
                new SolrTableScan(
                        scan.getCluster(),
                        scan.getTable(),
                        scan.solrTable,
                        fields));
    }

    private int[] getProjectFields(List<RexNode> exps) {
        final int[] fields = new int[exps.size()];
        for (int i = 0; i < exps.size(); i++) {
            final RexNode exp = exps.get(i);
            if (exp instanceof RexInputRef) {
                fields[i] = ((RexInputRef) exp).getIndex();
            } else {
                return null; // not a simple projection
            }
        }
        return fields;
    }
}

