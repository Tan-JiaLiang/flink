/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.ConcatDistinctAggFunction;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Before;
import org.junit.Test;

/** Test json serialization/deserialization for window aggregate. */
public class WindowAggregateJsonPlanTest extends TableTestBase {

    private StreamTableTestUtil util;
    private TableEnvironment tEnv;

    @Before
    public void setup() {
        util = streamTestUtil(TableConfig.getDefault());
        tEnv = util.getTableEnv();

        String srcTableDdl =
                "CREATE TABLE MyTable (\n"
                        + " a INT,\n"
                        + " b BIGINT,\n"
                        + " c VARCHAR,\n"
                        + " `rowtime` AS TO_TIMESTAMP(c),\n"
                        + " proctime as PROCTIME(),\n"
                        + " WATERMARK for `rowtime` AS `rowtime` - INTERVAL '1' SECOND\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values')\n";
        tEnv.executeSql(srcTableDdl);
    }

    @Test
    public void testEventTimeTumbleWindow() {
        tEnv.createFunction("concat_distinct_agg", ConcatDistinctAggFunction.class);
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + " b BIGINT,\n"
                        + " window_start TIMESTAMP(3),\n"
                        + " window_end TIMESTAMP(3),\n"
                        + " cnt BIGINT,\n"
                        + " sum_a INT,\n"
                        + " distinct_cnt BIGINT,\n"
                        + " concat_distinct STRING\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values')\n";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "insert into MySink select\n"
                        + "  b,\n"
                        + "  window_start,\n"
                        + "  window_end,\n"
                        + "  COUNT(*),\n"
                        + "  SUM(a),\n"
                        + "  COUNT(DISTINCT c),\n"
                        + "  concat_distinct_agg(c)\n"
                        + "FROM TABLE(\n"
                        + "   TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))\n"
                        + "GROUP BY b, window_start, window_end");
    }

    @Test
    public void testEventTimeTumbleWindowWithOffset() {
        tEnv.createFunction("concat_distinct_agg", ConcatDistinctAggFunction.class);
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + " b BIGINT,\n"
                        + " window_start TIMESTAMP(3),\n"
                        + " window_end TIMESTAMP(3),\n"
                        + " cnt BIGINT,\n"
                        + " sum_a INT,\n"
                        + " distinct_cnt BIGINT,\n"
                        + " concat_distinct STRING\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values')\n";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "insert into MySink select\n"
                        + "  b,\n"
                        + "  window_start,\n"
                        + "  window_end,\n"
                        + "  COUNT(*),\n"
                        + "  SUM(a),\n"
                        + "  COUNT(DISTINCT c),\n"
                        + "  concat_distinct_agg(c)\n"
                        + "FROM TABLE(\n"
                        + "   TUMBLE(\n"
                        + "     TABLE MyTable,\n"
                        + "     DESCRIPTOR(rowtime),\n"
                        + "     INTERVAL '5' SECOND,\n"
                        + "     INTERVAL '5' SECOND))\n"
                        + "GROUP BY b, window_start, window_end");
    }

    @Test
    public void testProcTimeTumbleWindow() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + " b BIGINT,\n"
                        + " window_end TIMESTAMP(3),\n"
                        + " cnt BIGINT\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values')\n";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "insert into MySink select\n"
                        + "  b,\n"
                        + "  window_end,\n"
                        + "  COUNT(*)\n"
                        + "FROM TABLE(\n"
                        + "   TUMBLE(TABLE MyTable, DESCRIPTOR(proctime), INTERVAL '15' MINUTE))\n"
                        + "GROUP BY b, window_start, window_end");
    }

    @Test
    public void testEventTimeHopWindow() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + " b BIGINT,\n"
                        + " cnt BIGINT,\n"
                        + " sum_a INT\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values')\n";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "insert into MySink select\n"
                        + "  b,\n"
                        + "  COUNT(c),\n"
                        + "  SUM(a)\n"
                        + "FROM TABLE(\n"
                        + "   HOP(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '5' SECOND, INTERVAL '10' SECOND))\n"
                        + "GROUP BY b, window_start, window_end");
    }

    @Test
    public void testEventTimeHopWindowWithOffset() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + " b BIGINT,\n"
                        + " cnt BIGINT,\n"
                        + " sum_a INT\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values')\n";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "insert into MySink select\n"
                        + "  b,\n"
                        + "  COUNT(c),\n"
                        + "  SUM(a)\n"
                        + "FROM TABLE(\n"
                        + "   HOP(\n"
                        + "     TABLE MyTable,\n"
                        + "     DESCRIPTOR(rowtime),\n"
                        + "     INTERVAL '5' SECOND,\n"
                        + "     INTERVAL '10' SECOND,\n"
                        + "     INTERVAL '5' SECOND))\n"
                        + "GROUP BY b, window_start, window_end");
    }

    @Test
    public void testProcTimeHopWindow() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + " b BIGINT,\n"
                        + " sum_a INT\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values')\n";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "insert into MySink select\n"
                        + "  b,\n"
                        + "  SUM(a)\n"
                        + "FROM TABLE(\n"
                        + "   HOP(TABLE MyTable, DESCRIPTOR(proctime), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))\n"
                        + "GROUP BY b, window_start, window_end");
    }

    @Test
    public void testEventTimeCumulateWindow() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + " b BIGINT,\n"
                        + " window_end TIMESTAMP(3),\n"
                        + " cnt BIGINT,\n"
                        + " sum_a INT\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values')\n";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "insert into MySink select\n"
                        + "  b,\n"
                        + "  window_end,\n"
                        + "  COUNT(c),\n"
                        + "  SUM(a)\n"
                        + "FROM TABLE(\n"
                        + "   CUMULATE(\n"
                        + "     TABLE MyTable,\n"
                        + "     DESCRIPTOR(rowtime),\n"
                        + "     INTERVAL '5' SECOND,\n"
                        + "     INTERVAL '15' SECOND))\n"
                        + "GROUP BY b, window_start, window_end");
    }

    @Test
    public void testEventTimeCumulateWindowWithOffset() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + " b BIGINT,\n"
                        + " window_end TIMESTAMP(3),\n"
                        + " cnt BIGINT,\n"
                        + " sum_a INT\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values')\n";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "insert into MySink select\n"
                        + "  b,\n"
                        + "  window_end,\n"
                        + "  COUNT(c),\n"
                        + "  SUM(a)\n"
                        + "FROM TABLE(\n"
                        + "   CUMULATE(\n"
                        + "     TABLE MyTable,\n"
                        + "     DESCRIPTOR(rowtime),\n"
                        + "     INTERVAL '5' SECOND,\n"
                        + "     INTERVAL '15' SECOND,\n"
                        + "     INTERVAL '15' SECOND))\n"
                        + "GROUP BY b, window_start, window_end");
    }

    @Test
    public void testProcTimeCumulateWindow() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + " b BIGINT,\n"
                        + " cnt BIGINT\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values')\n";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "insert into MySink select\n"
                        + "  b,\n"
                        + "  COUNT(c)\n"
                        + "FROM TABLE(\n"
                        + "   CUMULATE(\n"
                        + "     TABLE MyTable,\n"
                        + "     DESCRIPTOR(proctime),\n"
                        + "     INTERVAL '5' SECOND,\n"
                        + "     INTERVAL '15' SECOND))\n"
                        + "GROUP BY b, window_start, window_end");
    }

    @Test
    public void testEventTimeSlideWindow() {
        tEnv.createFunction("concat_distinct_agg", ConcatDistinctAggFunction.class);
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + " b BIGINT,\n"
                        + " window_start TIMESTAMP(3),\n"
                        + " window_end TIMESTAMP(3),\n"
                        + " cnt BIGINT,\n"
                        + " sum_a INT,\n"
                        + " distinct_cnt BIGINT,\n"
                        + " concat_distinct STRING\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values')\n";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "insert into MySink select\n"
                        + "  b,\n"
                        + "  window_start,\n"
                        + "  window_end,\n"
                        + "  COUNT(*),\n"
                        + "  SUM(a),\n"
                        + "  COUNT(DISTINCT c),\n"
                        + "  concat_distinct_agg(c)\n"
                        + "FROM TABLE(\n"
                        + "   SLIDE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))\n"
                        + "GROUP BY b, window_start, window_end");
    }

    @Test
    public void testProcTimeSlideWindow() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + " b BIGINT,\n"
                        + " window_end TIMESTAMP(3),\n"
                        + " cnt BIGINT\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values')\n";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "insert into MySink select\n"
                        + "  b,\n"
                        + "  window_end,\n"
                        + "  COUNT(*)\n"
                        + "FROM TABLE(\n"
                        + "   SLIDE(TABLE MyTable, DESCRIPTOR(proctime), INTERVAL '15' MINUTE))\n"
                        + "GROUP BY b, window_start, window_end");
    }

    @Test
    public void testDistinctSplitEnabled() {
        tEnv.getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, true);
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a bigint,\n"
                        + "  window_start timestamp(3),\n"
                        + "  window_end timestamp(3),\n"
                        + "  cnt_star bigint,\n"
                        + "  sum_b bigint,\n"
                        + "  cnt_distinct_c bigint\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);

        util.verifyJsonPlan(
                "insert into MySink select a, "
                        + "   window_start, "
                        + "   window_end, "
                        + "   count(*), "
                        + "   sum(b), "
                        + "   count(distinct c) AS uv "
                        + "FROM TABLE ("
                        + "   CUMULATE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR)) "
                        + "GROUP BY a, window_start, window_end");
    }
}
