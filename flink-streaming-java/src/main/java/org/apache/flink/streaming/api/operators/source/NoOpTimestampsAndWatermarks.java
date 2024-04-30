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

package org.apache.flink.streaming.api.operators.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ExceptionInChainedOperatorException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of {@link TimestampsAndWatermarks} where all watermarking/event-time operations
 * are no-ops. This should be used in execution contexts where no watermarks are needed, for example
 * in BATCH execution mode.
 *
 * @param <T> The type of the emitted records.
 */
@Internal
public class NoOpTimestampsAndWatermarks<T> implements TimestampsAndWatermarks<T> {

    private final TimestampAssigner<T> timestamps;
    private final InternalSourceReaderMetricGroup metricGroup;

    /** Creates a new {@link NoOpTimestampsAndWatermarks} with the given TimestampAssigner. */
    public NoOpTimestampsAndWatermarks(
            TimestampAssigner<T> timestamps, InternalSourceReaderMetricGroup metricGroup) {
        this.timestamps = checkNotNull(timestamps);
        this.metricGroup = checkNotNull(metricGroup);
    }

    @Override
    public ReaderOutput<T> createMainOutput(
            PushingAsyncDataInput.DataOutput<T> output, WatermarkUpdateListener watermarkEmitted) {
        checkNotNull(output);
        return new TimestampsOnlyOutput<>(output, timestamps, metricGroup);
    }

    @Override
    public void startPeriodicWatermarkEmits() {
        // no periodic watermarks
    }

    @Override
    public void stopPeriodicWatermarkEmits() {
        // no periodic watermarks
    }

    @Override
    public void emitImmediateWatermark(long wallClockTimestamp) {
        // do nothing
    }

    // ------------------------------------------------------------------------

    /**
     * A simple implementation of {@link SourceOutput} and {@link ReaderOutput} that extracts
     * timestamps but has no watermarking logic. Because only watermarking logic has state per
     * Source Split, the same instance gets shared across all Source Splits.
     *
     * @param <T> The type of the emitted records.
     */
    private static final class TimestampsOnlyOutput<T> implements ReaderOutput<T> {

        private final PushingAsyncDataInput.DataOutput<T> output;
        private final TimestampAssigner<T> timestampAssigner;
        private final InternalSourceReaderMetricGroup metricGroup;
        private final StreamRecord<T> reusingRecord;

        private TimestampsOnlyOutput(
                PushingAsyncDataInput.DataOutput<T> output,
                TimestampAssigner<T> timestampAssigner,
                InternalSourceReaderMetricGroup metricGroup) {

            this.output = output;
            this.timestampAssigner = timestampAssigner;
            this.metricGroup = metricGroup;
            this.reusingRecord = new StreamRecord<>(null);
        }

        @Override
        public void collect(T record) {
            collect(record, TimestampAssigner.NO_TIMESTAMP);
        }

        @Override
        public void collect(T record, long timestamp) {
            collect(record, timestamp, TimestampAssigner.NO_TIMESTAMP);
        }

        @Override
        public void collect(T record, long timestamp, long fetchTime) {
            try {
                long assignedTimestamp = timestampAssigner.extractTimestamp(record, timestamp);
                if (fetchTime != TimestampAssigner.NO_TIMESTAMP
                        && assignedTimestamp != TimestampAssigner.NO_TIMESTAMP) {
                    metricGroup.setCurrentFetchEventTimeLag(fetchTime - assignedTimestamp);
                }

                output.emitRecord(reusingRecord.replace(record, assignedTimestamp));
            } catch (ExceptionInChainedOperatorException e) {
                throw e;
            } catch (Exception e) {
                throw new ExceptionInChainedOperatorException(e);
            }
        }

        @Override
        public void emitWatermark(Watermark watermark) {
            // do nothing, this does not forward any watermarks manually emitted by the source
            // directly
        }

        @Override
        public void markIdle() {
            // do nothing, because without watermarks there is no idleness
        }

        @Override
        public void markActive() {
            // do nothing, it was never idle
        }

        @Override
        public SourceOutput<T> createOutputForSplit(String splitId) {
            // we don't need per-partition instances, because we do not generate watermarks
            return this;
        }

        @Override
        public void releaseOutputForSplit(String splitId) {
            // nothing to release, because we do not create per-partition instances
        }
    }
}
