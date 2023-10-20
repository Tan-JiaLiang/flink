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

package org.apache.flink.table.planner.runtime.stream.jsonplan;

import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.utils.AggregatePhaseStrategy;
import org.apache.flink.table.planner.utils.JsonPlanTestBase;

import org.apache.flink.types.Row;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/** Test for slide window aggregate json plan. */
@RunWith(Parameterized.class)
public class SlideWindowAggregateJsonITCase extends JsonPlanTestBase {

    @Parameterized.Parameters(name = "agg_phase = {0}")
    public static Object[] parameters() {
        return new Object[][]{
                new Object[]{AggregatePhaseStrategy.ONE_PHASE}
        };
    }

    @Parameterized.Parameter
    public AggregatePhaseStrategy aggPhase;

    private final static List<Row> TEST_DATE = Arrays.asList(
            Row.of("2020-10-10 00:00:01", "a", 1, "Hi"), // watermark: 2020-10-09 23:59:56, allowedLateness: 2020-10-09 23:59:46
            Row.of("2020-10-10 00:00:02", "a", 2, "Hello"), // watermark: 2020-10-09 23:59:57, allowedLateness: 2020-10-09 23:59:47
            Row.of("2020-10-10 00:00:05", "a", 5, "Comment#1"), // watermark: 2020-10-10 00:00:00, allowedLateness: 2020-10-09 23:59:50
            Row.of("2020-10-10 00:00:03", "a", 3, "Comment#1"), // out of order that event-time never appeared
            Row.of("2020-10-10 00:00:01", "a", 1, "Hi"), // out of order that event-time have already appeared
            Row.of("2020-10-10 00:00:00", "a", 0, "Hi"), // late event
            Row.of("2020-10-10 00:00:15", "b", 15, "Comment#2"), // watermark: 2020-10-10 00:00:10, allowedLateness: 2020-10-10 00:00:00
            Row.of("2020-10-10 00:00:03", "a", 3, "Comment#2"), // late event and event-time have already appeared, event-time > watermark - window size
            Row.of("2020-10-09 23:59:59", "a", 3, "Comment#3"), // not in window size, drop it
            Row.of("2020-10-10 00:00:19", "b", 19, "Comment#2"), // watermark: 2020-10-10 00:00:14, allowedLateness: 2020-10-10 00:00:04
            Row.of("2020-10-10 00:00:04", "a", 4, "Comment#3"), // late event and event-time never appeared, event-time = watermark - window size, drop it
            Row.of("2020-10-10 00:00:20", "b", 20, "Comment#2"), // watermark: 2020-10-10 00:00:15, allowedLateness: 2020-10-10 00:00:05
            Row.of("2020-10-10 00:00:05", "a", 5, "Hi"), // late event and event-time have already appeared, event-time = watermark - window size, drop it
            Row.of("2020-10-10 00:00:24", "b", 24, "Hi"), // watermark: 2020-10-10 00:00:19, allowedLateness: 2020-10-10 00:00:09
            Row.of("2020-10-10 00:00:11", "a", 11, "Hi"), // late event and event-time never appeared, event-time > watermark - window size
            Row.of("2020-10-10 00:00:35", "b", 35, "Comment#1"), // watermark: 2020-10-10 00:00:30, allowedLateness: 2020-10-10 00:00:20
            Row.of("2020-10-10 00:00:19", "a", 19, "Comment#4") // not in window size, drop it

            // 迟到数据场景
            // 1. 以前来过这个时间戳的数据，而且event-time > watermark - window size（计算）
            // 2. 以前没来过这个时间戳的数据，而且event-time > watermark - window size（计算）
            // 3. 以前来过这个时间戳的数据，而且event-time = watermark - window size（计算）
            // 4. 以前没来过这个时间戳的数据，而且event-time = watermark - window size（计算）
            // 5. 以前来过这个时间戳的数据，而且event-time < watermark - window size（丢弃）
            // 6. 以前没来过这个时间戳的数据，而且event-time < watermark - window size（丢弃）
    );

    @Before
    public void setup() throws Exception {
        super.setup();
        createTestValuesSourceTable(
                "MyTable",
                TEST_DATE,
                new String[]{
                        "ts STRING",
                        "`name` STRING",
                        "`int` INT",
                        "`string` STRING",
                        "`rowtime` AS TO_TIMESTAMP(`ts`)",
                        "WATERMARK for `rowtime` AS `rowtime` - INTERVAL '5' SECOND",
                },
                new HashMap<String, String>() {
                    {
                        put("enable-watermark-push-down", "true");
                        put("failing-source", "true");
                    }
                });
        tableEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY,
                        aggPhase.toString());
        tableEnv.getConfig()
                .set(CoreOptions.DEFAULT_PARALLELISM, 1);
    }

    @Test
    public void testEventTimeSlideWindow() throws Exception {
        createTestValuesSinkTable(
                "MySink",
                "name STRING",
                "window_start TIMESTAMP(3)",
                "window_end TIMESTAMP(3)",
                "cnt BIGINT",
                "sum_int INT",
                "distinct_cnt BIGINT");
        compileSqlAndExecutePlan(
                "insert into MySink select\n"
                        + "  name,\n"
                        + "  window_start,\n"
                        + "  window_end,\n"
                        + "  COUNT(*),\n"
                        + "  SUM(`int`),\n"
                        + "  COUNT(DISTINCT `string`)\n"
                        + "FROM TABLE(\n"
                        + "   SLIDE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '10' SECOND))\n"
                        + "GROUP BY name, window_start, window_end")
                .await();

        List<String> result = TestValuesTableFactory.getResults("MySink");
        assertResult(
                Arrays.asList(
                        "+I[a, 2020-10-09T23:59:51, 2020-10-10T00:00:01, 2, 2, 1]",
                        "+I[a, 2020-10-09T23:59:52, 2020-10-10T00:00:02, 3, 4, 2]",
                        "+I[a, 2020-10-09T23:59:53, 2020-10-10T00:00:03, 4, 7, 3]",
                        "+I[a, 2020-10-09T23:59:55, 2020-10-10T00:00:05, 5, 12, 3]",
                        "+I[a, 2020-10-10T00:00:01, 2020-10-10T00:00:11, 3, 10, 2]",
                        "+I[a, 2020-10-10T00:00:02, 2020-10-10T00:00:12, 2, 8, 1]",
                        "+I[a, 2020-10-10T00:00:03, 2020-10-10T00:00:13, 1, 5, 1]",
                        "+I[a, 2020-10-10T00:00:05, 2020-10-10T00:00:15, 0, null, 0]",
                        "+I[b, 2020-10-10T00:00:05, 2020-10-10T00:00:15, 1, 15, 1]",
                        "+I[b, 2020-10-10T00:00:09, 2020-10-10T00:00:19, 2, 34, 1]",
                        "+I[b, 2020-10-10T00:00:10, 2020-10-10T00:00:20, 3, 54, 1]",
                        "+I[b, 2020-10-10T00:00:14, 2020-10-10T00:00:24, 4, 78, 2]",
                        "+I[b, 2020-10-10T00:00:15, 2020-10-10T00:00:25, 3, 63, 2]",
                        "+I[b, 2020-10-10T00:00:19, 2020-10-10T00:00:29, 2, 44, 2]",
                        "+I[b, 2020-10-10T00:00:20, 2020-10-10T00:00:30, 1, 24, 1]",
                        "+I[b, 2020-10-10T00:00:24, 2020-10-10T00:00:34, 0, null, 0]",
                        "+I[b, 2020-10-10T00:00:25, 2020-10-10T00:00:35, 1, 35, 1]",
                        "+I[b, 2020-10-10T00:00:35, 2020-10-10T00:00:45, 0, null, 0]"),
                result);
    }

    @Test
    public void testEventTimeSlideWindowAllowedLateness() throws Exception {
        createTestValuesSinkTable(
                "MySink",
                "name STRING",
                "window_start TIMESTAMP(3)",
                "window_end TIMESTAMP(3)",
                "cnt BIGINT",
                "sum_int INT",
                "distinct_cnt BIGINT");
        compileSqlAndExecutePlan(
                "insert into MySink select\n"
                        + "  name,\n"
                        + "  window_start,\n"
                        + "  window_end,\n"
                        + "  COUNT(*),\n"
                        + "  SUM(`int`),\n"
                        + "  COUNT(DISTINCT `string`)\n"
                        + "FROM TABLE(\n"
                        + "   SLIDE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '10' SECOND, true))\n"
                        + "GROUP BY name, window_start, window_end")
                .await();

        List<String> result = TestValuesTableFactory.getResults("MySink");
        assertResult(
                Arrays.asList(
                        "+I[a, 2020-10-09T23:59:51, 2020-10-10T00:00:01, 3, 2, 1]",
                        "+I[a, 2020-10-09T23:59:52, 2020-10-10T00:00:02, 4, 4, 2]",
                        "+I[a, 2020-10-09T23:59:53, 2020-10-10T00:00:03, 5, 7, 3]",
                        "+I[a, 2020-10-09T23:59:55, 2020-10-10T00:00:05, 6, 12, 3]",
                        "+I[a, 2020-10-10T00:00, 2020-10-10T00:00:10, 5, 12, 3]",
                        "+I[a, 2020-10-10T00:00:01, 2020-10-10T00:00:11, 4, 13, 3]",
                        "+I[a, 2020-10-10T00:00:02, 2020-10-10T00:00:12, 3, 11, 2]",
                        "+I[a, 2020-10-10T00:00:03, 2020-10-10T00:00:13, 1, 5, 1]",
                        "+I[a, 2020-10-10T00:00:05, 2020-10-10T00:00:15, 0, null, 0]",
                        "+I[a, 2020-10-10T00:00:11, 2020-10-10T00:00:21, 0, null, 0]",
                        "+I[b, 2020-10-10T00:00:05, 2020-10-10T00:00:15, 1, 15, 1]",
                        "+I[b, 2020-10-10T00:00:09, 2020-10-10T00:00:19, 2, 34, 1]",
                        "+I[b, 2020-10-10T00:00:10, 2020-10-10T00:00:20, 3, 54, 1]",
                        "+I[b, 2020-10-10T00:00:14, 2020-10-10T00:00:24, 4, 78, 2]",
                        "+I[b, 2020-10-10T00:00:15, 2020-10-10T00:00:25, 3, 63, 2]",
                        "+I[b, 2020-10-10T00:00:19, 2020-10-10T00:00:29, 2, 44, 2]",
                        "+I[b, 2020-10-10T00:00:20, 2020-10-10T00:00:30, 1, 24, 1]",
                        "+I[b, 2020-10-10T00:00:24, 2020-10-10T00:00:34, 0, null, 0]",
                        "+I[b, 2020-10-10T00:00:25, 2020-10-10T00:00:35, 1, 35, 1]",
                        "+I[b, 2020-10-10T00:00:35, 2020-10-10T00:00:45, 0, null, 0]"),
                result);
    }
}
