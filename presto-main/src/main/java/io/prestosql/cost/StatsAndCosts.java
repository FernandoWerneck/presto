/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.prestosql.cost;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.graph.Traverser;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.PlanNodeId;

import java.util.Map;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class StatsAndCosts
{
    private static final StatsAndCosts EMPTY = new StatsAndCosts(ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of());

    private final Map<PlanNodeId, PlanNodeStatsEstimate> stats;
    private final Map<PlanNodeId, PlanNodeCostEstimate> nodeCosts;
    private final Map<PlanNodeId, PlanNodeCostEstimate> cumulativeCosts;

    public static StatsAndCosts empty()
    {
        return EMPTY;
    }

    @JsonCreator
    public StatsAndCosts(
            @JsonProperty("stats") Map<PlanNodeId, PlanNodeStatsEstimate> stats,
            @JsonProperty("nodeCosts") Map<PlanNodeId, PlanNodeCostEstimate> nodeCosts,
            @JsonProperty("cumulativeCosts") Map<PlanNodeId, PlanNodeCostEstimate> cumulativeCosts)
    {
        this.stats = ImmutableMap.copyOf(requireNonNull(stats, "stats is null"));
        this.nodeCosts = ImmutableMap.copyOf(requireNonNull(nodeCosts, "nodeCosts is null"));
        this.cumulativeCosts = ImmutableMap.copyOf(requireNonNull(cumulativeCosts, "cumulativeCosts is null"));
    }

    @JsonProperty
    public Map<PlanNodeId, PlanNodeStatsEstimate> getStats()
    {
        return stats;
    }

    @JsonProperty
    public Map<PlanNodeId, PlanNodeCostEstimate> getNodeCosts()
    {
        return nodeCosts;
    }

    @JsonProperty
    public Map<PlanNodeId, PlanNodeCostEstimate> getCumulativeCosts()
    {
        return cumulativeCosts;
    }

    public StatsAndCosts getForSubplan(PlanNode root)
    {
        Iterable<PlanNode> planIterator = Traverser.forTree(PlanNode::getSources)
                .depthFirstPreOrder(root);
        ImmutableMap.Builder<PlanNodeId, PlanNodeStatsEstimate> filteredStats = ImmutableMap.builder();
        ImmutableMap.Builder<PlanNodeId, PlanNodeCostEstimate> filteredNodeCosts = ImmutableMap.builder();
        ImmutableMap.Builder<PlanNodeId, PlanNodeCostEstimate> filteredCumulativeCosts = ImmutableMap.builder();
        for (PlanNode node : planIterator) {
            if (stats.containsKey(node.getId())) {
                filteredStats.put(node.getId(), stats.get(node.getId()));
            }
            if (nodeCosts.containsKey(node.getId())) {
                filteredNodeCosts.put(node.getId(), nodeCosts.get(node.getId()));
            }
            if (cumulativeCosts.containsKey(node.getId())) {
                filteredCumulativeCosts.put(node.getId(), cumulativeCosts.get(node.getId()));
            }
        }
        return new StatsAndCosts(filteredStats.build(), filteredNodeCosts.build(), filteredCumulativeCosts.build());
    }

    public static StatsAndCosts create(PlanNode root, StatsProvider statsProvider, Function<PlanNode, PlanNodeCostEstimate> nodeCostProvider, CostProvider costProvider)
    {
        Iterable<PlanNode> planIterator = Traverser.forTree(PlanNode::getSources)
                .depthFirstPreOrder(root);
        ImmutableMap.Builder<PlanNodeId, PlanNodeStatsEstimate> stats = ImmutableMap.builder();
        ImmutableMap.Builder<PlanNodeId, PlanNodeCostEstimate> nodeCosts = ImmutableMap.builder();
        ImmutableMap.Builder<PlanNodeId, PlanNodeCostEstimate> cumulativeCosts = ImmutableMap.builder();
        for (PlanNode node : planIterator) {
            stats.put(node.getId(), statsProvider.getStats(node));
            nodeCosts.put(node.getId(), nodeCostProvider.apply(node));
            cumulativeCosts.put(node.getId(), costProvider.getCumulativeCost(node));
        }
        return new StatsAndCosts(stats.build(), nodeCosts.build(), cumulativeCosts.build());
    }
}
