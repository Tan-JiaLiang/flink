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
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.types.Row;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for window aggregate json plan. */
@RunWith(Parameterized.class)
public class HopV2WindowAggregateJsonITCase extends JsonPlanTestBase {

    @Parameterized.Parameters(name = "agg_phase = {0}")
    public static Object[] parameters() {
        return new Object[][]{
                new Object[]{AggregatePhaseStrategy.ONE_PHASE},
        };
    }

    @Parameterized.Parameter
    public AggregatePhaseStrategy aggPhase;

    private final static List<Row> TEST_DATE = Arrays.asList(
            // watermark: 2020-10-09 23:59:54, allowedLateness: 2020-10-09 23:59:45
            Row.of("2020-10-09 23:59:59", "a", 1, "Hi"), // window_slice: 2020-10-09 23:59:55
            // watermark: 2020-10-09 23:59:56, allowedLateness: 2020-10-09 23:59:50
            Row.of("2020-10-10 00:00:01", "a", 1, "Hi"), // window_slice: 2020-10-10 00:00:00
            // watermark: 2020-10-09 23:59:57, allowedLateness: 2020-10-09 23:59:50
            Row.of("2020-10-10 00:00:02", "a", 2, "Hello"), // window_slice: 2020-10-10 00:00:00
            // watermark: 2020-10-10 00:00:00, allowedLateness: 2020-10-09 23:59:55
            Row.of("2020-10-10 00:00:05", "a", 5, "Comment#1"), // window_slice: 2020-10-10 00:00:00
            Row.of("2020-10-10 00:00:03", "a", 3, "Comment#1"), // window_slice: 2020-10-10 00:00:00, out of order that event-time never appeared
            Row.of("2020-10-10 00:00:01", "a", 1, "Hi"), // window_slice: 2020-10-10 00:00:00, out of order that event-time have already appeared
            Row.of("2020-10-10 00:00:00", "a", 0, "Hi"), // window_slice: 2020-10-10 00:00:00, late event(event-time=watermark)
            // watermark: 2020-10-10 00:00:10, allowedLateness: 2020-10-10 00:00:05
            Row.of("2020-10-10 00:00:15", "b", 15, "Comment#2"), // window_slice: 2020-10-10 00:00:15
            Row.of("2020-10-10 00:00:04", "a", 4, "Comment#1"), // window_slice: 2020-10-10 00:00:00, out of window size, drop it
            Row.of("2020-10-10 00:00:03", "a", 3, "Comment#2"), // window_slice: 2020-10-10 00:00:00, out of window size, drop it
            Row.of("2020-10-10 00:00:05", "a", 5, "Comment#2"), // window_slice: 2020-10-10 00:00:00, late event
            Row.of("2020-10-10 00:00:00", "a", 0, "Comment#2"), // window_slice: 2020-10-10 00:00:00, out of window size, drop it, drop it
            Row.of("2020-10-10 00:00:10", "a", 10, "Comment#10"), // window_slice: 2020-10-10 00:00:10, late event
            Row.of("2020-10-10 00:00:08", "a", 8, "Comment#10"), // window_slice: 2020-10-10 00:00:10, late event
            Row.of("2020-10-10 00:00:08", "a", 8, "Comment#8"), // window_slice: 2020-10-10 00:00:10, late event
            Row.of("2020-10-09 23:59:59", "a", 3, "Comment#3"), // window_slice: 2020-10-09 23:59:55, out of window size, drop it
            // watermark: 2020-10-10 00:00:11, allowedLateness: 2020-10-10 00:00:05
            Row.of("2020-10-10 00:00:18", "b", 18, "Comment#2"), // window_slice: 2020-10-10 00:00:15
            // watermark: 2020-10-10 00:00:16, allowedLateness: 2020-10-10 00:00:10
            Row.of("2020-10-10 00:00:21", "b", 21, "Comment#2"), // window_slice: 2020-10-10 00:00:20
            // watermark: 2020-10-10 00:00:29, allowedLateness: 2020-10-10 00:00:20
            Row.of("2020-10-10 00:00:34", "b", 34, "Comment#1") // window_slice: 2020-10-10 00:00:30
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
        tableEnv.getConfig().set(CoreOptions.DEFAULT_PARALLELISM, 1);
    }

    @Test
    public void testEventTimeHopV2Window() throws Exception {
        createTestValuesSinkTable(
                "MySink",
                "name STRING",
                "window_start TIMESTAMP(3)",
                "window_time TIMESTAMP(3)",
                "window_end TIMESTAMP(3)",
                "cnt BIGINT",
                "sum_int INT",
                "distinct_cnt BIGINT");
        compileSqlAndExecutePlan(
                "insert into MySink select\n"
                        + "  name,\n"
                        + "  window_start,\n"
                        + "  window_time,\n"
                        + "  window_end,\n"
                        + "  COUNT(*),\n"
                        + "  SUM(`int`),\n"
                        + "  COUNT(DISTINCT `string`)\n"
                        + "FROM TABLE(\n"
                        + "   HOPV2(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '5' SECOND, INTERVAL '10' SECOND))\n"
                        + "GROUP BY name, window_start, window_time, window_end")
                .await();

        List<String> result = TestValuesTableFactory.getResults("MySink");
        assertResult(
                Arrays.asList(
                        "+I[a, 2020-10-09T23:59:50, 2020-10-09T23:59:59, 2020-10-10T00:00, 1, 1, 1]",
                        "+I[a, 2020-10-09T23:59:55, 2020-10-10T00:00:01, 2020-10-10T00:00:05, 3, 3, 1]",
                        "+I[a, 2020-10-09T23:59:55, 2020-10-10T00:00:02, 2020-10-10T00:00:05, 4, 5, 2]",
                        "+I[a, 2020-10-09T23:59:55, 2020-10-10T00:00:03, 2020-10-10T00:00:05, 5, 8, 3]",
                        "+I[a, 2020-10-09T23:59:55, 2020-10-10T00:00:04.999, 2020-10-10T00:00:05, 5, 8, 3]",
                        "+I[a, 2020-10-10T00:00, 2020-10-10T00:00:05, 2020-10-10T00:00:10, 5, 12, 3]",
                        "+I[a, 2020-10-10T00:00, 2020-10-10T00:00:09.999, 2020-10-10T00:00:10, 5, 12, 3]",
                        "+I[a, 2020-10-10T00:00:05, 2020-10-10T00:00:10, 2020-10-10T00:00:15, 1, 5, 1]",
                        "+I[a, 2020-10-10T00:00:05, 2020-10-10T00:00:14.999, 2020-10-10T00:00:15, 1, 5, 1]",
                        "+I[a, 2020-10-10T00:00:10, 2020-10-10T00:00:15, 2020-10-10T00:00:20, 0, null, 0]",
                        "+I[b, 2020-10-10T00:00:10, 2020-10-10T00:00:15, 2020-10-10T00:00:20, 1, 15, 1]",
                        "+I[b, 2020-10-10T00:00:10, 2020-10-10T00:00:18, 2020-10-10T00:00:20, 2, 33, 1]",
                        "+I[b, 2020-10-10T00:00:15, 2020-10-10T00:00:21, 2020-10-10T00:00:25, 3, 54, 1]",
                        "+I[b, 2020-10-10T00:00:15, 2020-10-10T00:00:24.999, 2020-10-10T00:00:25, 3, 54, 1]",
                        "+I[b, 2020-10-10T00:00:20, 2020-10-10T00:00:25, 2020-10-10T00:00:30, 1, 21, 1]",
                        "+I[b, 2020-10-10T00:00:20, 2020-10-10T00:00:29.999, 2020-10-10T00:00:30, 1, 21, 1]",
                        "+I[b, 2020-10-10T00:00:25, 2020-10-10T00:00:30, 2020-10-10T00:00:35, 0, null, 0]",
                        "+I[b, 2020-10-10T00:00:25, 2020-10-10T00:00:34, 2020-10-10T00:00:35, 1, 34, 1]",
                        "+I[b, 2020-10-10T00:00:30, 2020-10-10T00:00:39.999, 2020-10-10T00:00:40, 1, 34, 1]",
                        "+I[b, 2020-10-10T00:00:35, 2020-10-10T00:00:40, 2020-10-10T00:00:45, 0, null, 0]"
                        ),
                result);
    }

    @Test
    public void testEventTimeHopV2WindowAllowedLateness() throws Exception {
        createTestValuesSinkTable(
                "MySink",
                "name STRING",
                "window_start TIMESTAMP(3)",
                "window_time TIMESTAMP(3)",
                "window_end TIMESTAMP(3)",
                "cnt BIGINT",
                "sum_int INT",
                "distinct_cnt BIGINT");
        compileSqlAndExecutePlan(
                "insert into MySink select\n"
                        + "  name,\n"
                        + "  window_start,\n"
                        + "  window_time,\n"
                        + "  window_end,\n"
                        + "  COUNT(*),\n"
                        + "  SUM(`int`),\n"
                        + "  COUNT(DISTINCT `string`)\n"
                        + "FROM TABLE(\n"
                        + "   HOPV2(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '5' SECOND, INTERVAL '10' SECOND, true))\n"
                        + "GROUP BY name, window_start, window_time, window_end")
                .await();

        List<String> result = TestValuesTableFactory.getResults("MySink");
        assertResult(
                Arrays.asList(
                        "+I[a, 2020-10-09T23:59:50, 2020-10-09T23:59:59, 2020-10-10T00:00, 1, 1, 1]",
                        "+I[a, 2020-10-09T23:59:55, 2020-10-10T00:00:01, 2020-10-10T00:00:05, 4, 3, 1]",
                        "+I[a, 2020-10-09T23:59:55, 2020-10-10T00:00:02, 2020-10-10T00:00:05, 5, 5, 2]",
                        "+I[a, 2020-10-09T23:59:55, 2020-10-10T00:00:03, 2020-10-10T00:00:05, 6, 8, 3]",
                        "+I[a, 2020-10-09T23:59:55, 2020-10-10T00:00:04.999, 2020-10-10T00:00:05, 6, 8, 3]",
                        "+I[a, 2020-10-10T00:00, 2020-10-10T00:00:05, 2020-10-10T00:00:10, 6, 12, 3]",
                        "+I[a, 2020-10-10T00:00, 2020-10-10T00:00:09.999, 2020-10-10T00:00:10, 6, 12, 3]",
                        "+I[a, 2020-10-10T00:00:05, 2020-10-10T00:00:10, 2020-10-10T00:00:15, 1, 5, 1]",
                        "+I[a, 2020-10-10T00:00:05, 2020-10-10T00:00:14.999, 2020-10-10T00:00:15, 5, 36, 4]",
                        "+I[a, 2020-10-10T00:00:10, 2020-10-10T00:00:15, 2020-10-10T00:00:20, 1, 10, 1]",
                        "+I[a, 2020-10-10T00:00:10, 2020-10-10T00:00:19.999, 2020-10-10T00:00:20, 1, 10, 1]",
                        "+I[a, 2020-10-10T00:00:15, 2020-10-10T00:00:20, 2020-10-10T00:00:25, 0, null, 0]",
                        "+I[b, 2020-10-10T00:00:10, 2020-10-10T00:00:15, 2020-10-10T00:00:20, 1, 15, 1]",
                        "+I[b, 2020-10-10T00:00:10, 2020-10-10T00:00:18, 2020-10-10T00:00:20, 2, 33, 1]",
                        "+I[b, 2020-10-10T00:00:15, 2020-10-10T00:00:21, 2020-10-10T00:00:25, 3, 54, 1]",
                        "+I[b, 2020-10-10T00:00:15, 2020-10-10T00:00:24.999, 2020-10-10T00:00:25, 3, 54, 1]",
                        "+I[b, 2020-10-10T00:00:20, 2020-10-10T00:00:25, 2020-10-10T00:00:30, 1, 21, 1]",
                        "+I[b, 2020-10-10T00:00:20, 2020-10-10T00:00:29.999, 2020-10-10T00:00:30, 1, 21, 1]",
                        "+I[b, 2020-10-10T00:00:25, 2020-10-10T00:00:30, 2020-10-10T00:00:35, 0, null, 0]",
                        "+I[b, 2020-10-10T00:00:25, 2020-10-10T00:00:34, 2020-10-10T00:00:35, 1, 34, 1]",
                        "+I[b, 2020-10-10T00:00:30, 2020-10-10T00:00:39.999, 2020-10-10T00:00:40, 1, 34, 1]",
                        "+I[b, 2020-10-10T00:00:35, 2020-10-10T00:00:40, 2020-10-10T00:00:45, 0, null, 0]"
                ),
                result);
    }

    @Test
    public void testWindowStart() {
        // slide=5, size=10
        assertThat(getWindowStart(
                toEpochMilli("2020-10-09T23:59:55"),
                TimeUnit.SECONDS.toMillis(5),
                TimeUnit.SECONDS.toMillis(10)))
                .isEqualTo(toEpochMilli("2020-10-09T23:59:50"));
        assertThat(getWindowStart(
                toEpochMilli("2020-10-10T00:00:00"),
                TimeUnit.SECONDS.toMillis(5),
                TimeUnit.SECONDS.toMillis(10)))
                .isEqualTo(toEpochMilli("2020-10-09T23:59:55"));
        assertThat(getWindowStart(
                toEpochMilli("2020-10-10T00:00:01"),
                TimeUnit.SECONDS.toMillis(5),
                TimeUnit.SECONDS.toMillis(10)))
                .isEqualTo(toEpochMilli("2020-10-09T23:59:55"));
        assertThat(getWindowStart(
                toEpochMilli("2020-10-10T00:00:02"),
                TimeUnit.SECONDS.toMillis(5),
                TimeUnit.SECONDS.toMillis(10)))
                .isEqualTo(toEpochMilli("2020-10-09T23:59:55"));
        assertThat(getWindowStart(
                toEpochMilli("2020-10-10T00:00:05"),
                TimeUnit.SECONDS.toMillis(5),
                TimeUnit.SECONDS.toMillis(10)))
                .isEqualTo(toEpochMilli("2020-10-10T00:00:00"));
        assertThat(getWindowStart(
                toEpochMilli("2020-10-10T00:00:11"),
                TimeUnit.SECONDS.toMillis(5),
                TimeUnit.SECONDS.toMillis(10)))
                .isEqualTo(toEpochMilli("2020-10-10T00:00:05"));
        assertThat(getWindowStart(
                toEpochMilli("2020-10-10T00:00:08"),
                TimeUnit.SECONDS.toMillis(5),
                TimeUnit.SECONDS.toMillis(10)))
                .isEqualTo(toEpochMilli("2020-10-10T00:00:00"));

        // slide=5, size=15
        assertThat(getWindowStart(
                toEpochMilli("2020-10-09T23:59:55"),
                TimeUnit.SECONDS.toMillis(5),
                TimeUnit.SECONDS.toMillis(15)))
                .isEqualTo(toEpochMilli("2020-10-09T23:59:45"));
        assertThat(getWindowStart(
                toEpochMilli("2020-10-10T00:00:00"),
                TimeUnit.SECONDS.toMillis(5),
                TimeUnit.SECONDS.toMillis(15)))
                .isEqualTo(toEpochMilli("2020-10-09T23:59:50"));
        assertThat(getWindowStart(
                toEpochMilli("2020-10-10T00:00:01"),
                TimeUnit.SECONDS.toMillis(5),
                TimeUnit.SECONDS.toMillis(15)))
                .isEqualTo(toEpochMilli("2020-10-09T23:59:50"));
        assertThat(getWindowStart(
                toEpochMilli("2020-10-10T00:00:02"),
                TimeUnit.SECONDS.toMillis(5),
                TimeUnit.SECONDS.toMillis(15)))
                .isEqualTo(toEpochMilli("2020-10-09T23:59:50"));
        assertThat(getWindowStart(
                toEpochMilli("2020-10-10T00:00:05"),
                TimeUnit.SECONDS.toMillis(5),
                TimeUnit.SECONDS.toMillis(15)))
                .isEqualTo(toEpochMilli("2020-10-09T23:59:55"));
        assertThat(getWindowStart(
                toEpochMilli("2020-10-10T00:00:11"),
                TimeUnit.SECONDS.toMillis(5),
                TimeUnit.SECONDS.toMillis(15)))
                .isEqualTo(toEpochMilli("2020-10-10T00:00:00"));
        assertThat(getWindowStart(
                toEpochMilli("2020-10-10T00:00:08"),
                TimeUnit.SECONDS.toMillis(5),
                TimeUnit.SECONDS.toMillis(15)))
                .isEqualTo(toEpochMilli("2020-10-09T23:59:55"));
    }

    @Test
    public void testGetCurrentSlice() {
        // slide=5
        assertThat(getCurrentSlice(
                toEpochMilli("2020-10-09T23:59:55"),
                TimeUnit.SECONDS.toMillis(5)))
                .isEqualTo(toEpochMilli("2020-10-09T23:59:55"));
        assertThat(getCurrentSlice(
                toEpochMilli("2020-10-10T00:00:00"),
                TimeUnit.SECONDS.toMillis(5)))
                .isEqualTo(toEpochMilli("2020-10-10T00:00:00"));
        assertThat(getCurrentSlice(
                toEpochMilli("2020-10-10T00:00:01"),
                TimeUnit.SECONDS.toMillis(5)))
                .isEqualTo(toEpochMilli("2020-10-10T00:00:00"));
        assertThat(getCurrentSlice(
                toEpochMilli("2020-10-10T00:00:02"),
                TimeUnit.SECONDS.toMillis(5)))
                .isEqualTo(toEpochMilli("2020-10-10T00:00:00"));
        assertThat(getCurrentSlice(
                toEpochMilli("2020-10-10T00:00:05"),
                TimeUnit.SECONDS.toMillis(5)))
                .isEqualTo(toEpochMilli("2020-10-10T00:00:05"));
        assertThat(getCurrentSlice(
                toEpochMilli("2020-10-10T00:00:11"),
                TimeUnit.SECONDS.toMillis(5)))
                .isEqualTo(toEpochMilli("2020-10-10T00:00:10"));
        assertThat(getCurrentSlice(
                toEpochMilli("2020-10-10T00:00:08"),
                TimeUnit.SECONDS.toMillis(5)))
                .isEqualTo(toEpochMilli("2020-10-10T00:00:05"));
        assertThat(getCurrentSlice(
                toEpochMilli("2020-10-10T00:00:15"),
                TimeUnit.SECONDS.toMillis(5)))
                .isEqualTo(toEpochMilli("2020-10-10T00:00:15"));

        // slide=10
        assertThat(getCurrentSlice(
                toEpochMilli("2020-10-09T23:59:55"),
                TimeUnit.SECONDS.toMillis(5)))
                .isEqualTo(toEpochMilli("2020-10-09T23:59:55"));
        assertThat(getCurrentSlice(
                toEpochMilli("2020-10-09T23:59:50"),
                TimeUnit.SECONDS.toMillis(5)))
                .isEqualTo(toEpochMilli("2020-10-09T23:59:50"));
        assertThat(getCurrentSlice(
                toEpochMilli("2020-10-10T00:00:00"),
                TimeUnit.SECONDS.toMillis(10)))
                .isEqualTo(toEpochMilli("2020-10-10T00:00:00"));
        assertThat(getCurrentSlice(
                toEpochMilli("2020-10-10T00:00:01"),
                TimeUnit.SECONDS.toMillis(10)))
                .isEqualTo(toEpochMilli("2020-10-10T00:00:00"));
        assertThat(getCurrentSlice(
                toEpochMilli("2020-10-10T00:00:02"),
                TimeUnit.SECONDS.toMillis(10)))
                .isEqualTo(toEpochMilli("2020-10-10T00:00:00"));
        assertThat(getCurrentSlice(
                toEpochMilli("2020-10-10T00:00:05"),
                TimeUnit.SECONDS.toMillis(10)))
                .isEqualTo(toEpochMilli("2020-10-10T00:00:00"));
        assertThat(getCurrentSlice(
                toEpochMilli("2020-10-10T00:00:11"),
                TimeUnit.SECONDS.toMillis(10)))
                .isEqualTo(toEpochMilli("2020-10-10T00:00:10"));
        assertThat(getCurrentSlice(
                toEpochMilli("2020-10-10T00:00:08"),
                TimeUnit.SECONDS.toMillis(10)))
                .isEqualTo(toEpochMilli("2020-10-10T00:00:00"));
        assertThat(getCurrentSlice(
                toEpochMilli("2020-10-10T00:00:15"),
                TimeUnit.SECONDS.toMillis(10)))
                .isEqualTo(toEpochMilli("2020-10-10T00:00:10"));
    }

    private long getWindowStart(long timestamp, long slide, long size) {
        return getCurrentSlice(timestamp - size + slide, slide);
    }

    private long getCurrentSlice(long timestamp, long slide) {
        return TimeWindow.getWindowStartWithOffset(timestamp, 0, slide);
    }

    private long toEpochMilli(String datetime) {
        return LocalDateTime.parse(datetime).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }
}
