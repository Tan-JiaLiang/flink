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
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkOutputMultiplexer;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * An implementation of {@link TimestampsAndWatermarks} that does periodic watermark emission and
 * keeps track of watermarks on a per-split basis. This should be used in execution contexts where
 * watermarks are important for efficiency/correctness, for example in STREAMING execution mode.
 *
 * @param <T> The type of the emitted records.
 */
@Internal
public class ProgressiveTimestampsAndWatermarks<T> implements TimestampsAndWatermarks<T> {

    private final TimestampAssigner<T> timestampAssigner;

    private final WatermarkGeneratorSupplier<T> watermarksFactory;

    private final WatermarkGeneratorSupplier.Context watermarksContext;

    private final ProcessingTimeService timeService;

    private final InternalSourceReaderMetricGroup metricGroup;

    private final long periodicWatermarkInterval;

    @Nullable private SplitLocalOutputs<T> currentPerSplitOutputs;

    @Nullable private StreamingReaderOutput<T> currentMainOutput;

    @Nullable private ScheduledFuture<?> periodicEmitHandle;

    public ProgressiveTimestampsAndWatermarks(
            TimestampAssigner<T> timestampAssigner,
            WatermarkGeneratorSupplier<T> watermarksFactory,
            WatermarkGeneratorSupplier.Context watermarksContext,
            ProcessingTimeService timeService,
            InternalSourceReaderMetricGroup metricGroup,
            Duration periodicWatermarkInterval) {

        this.timestampAssigner = timestampAssigner;
        this.watermarksFactory = watermarksFactory;
        this.watermarksContext = watermarksContext;
        this.timeService = timeService;
        this.metricGroup = metricGroup;

        long periodicWatermarkIntervalMillis;
        try {
            periodicWatermarkIntervalMillis = periodicWatermarkInterval.toMillis();
        } catch (ArithmeticException ignored) {
            // long integer overflow
            periodicWatermarkIntervalMillis = Long.MAX_VALUE;
        }
        this.periodicWatermarkInterval = periodicWatermarkIntervalMillis;
    }

    // ------------------------------------------------------------------------

    @Override
    public ReaderOutput<T> createMainOutput(
            PushingAsyncDataInput.DataOutput<T> output,
            WatermarkUpdateListener watermarkUpdateListener) {
        // At the moment, we assume only one output is ever created!
        // This assumption is strict, currently, because many of the classes in this implementation
        // do not
        // support re-assigning the underlying output
        checkState(
                currentMainOutput == null && currentPerSplitOutputs == null,
                "already created a main output");

        final WatermarkOutput watermarkOutput =
                new WatermarkToDataOutput(output, watermarkUpdateListener);
        IdlenessManager idlenessManager = new IdlenessManager(watermarkOutput);

        final WatermarkGenerator<T> watermarkGenerator =
                watermarksFactory.createWatermarkGenerator(watermarksContext);

        currentPerSplitOutputs =
                new SplitLocalOutputs<>(
                        output,
                        idlenessManager.getSplitLocalOutput(),
                        watermarkUpdateListener,
                        timestampAssigner,
                        watermarksFactory,
                        watermarksContext,
                        metricGroup);

        currentMainOutput =
                new StreamingReaderOutput<>(
                        output,
                        idlenessManager.getMainOutput(),
                        timestampAssigner,
                        watermarkGenerator,
                        currentPerSplitOutputs,
                        metricGroup);

        return currentMainOutput;
    }

    @Override
    public void startPeriodicWatermarkEmits() {
        checkState(periodicEmitHandle == null, "periodic emitter already started");

        if (periodicWatermarkInterval == 0) {
            // a value of zero means not activated
            return;
        }

        periodicEmitHandle =
                timeService.scheduleWithFixedDelay(
                        this::emitImmediateWatermark,
                        periodicWatermarkInterval,
                        periodicWatermarkInterval);
    }

    @Override
    public void stopPeriodicWatermarkEmits() {
        if (periodicEmitHandle != null) {
            periodicEmitHandle.cancel(false);
            periodicEmitHandle = null;
        }
    }

    @Override
    public void emitImmediateWatermark(@SuppressWarnings("unused") long wallClockTimestamp) {
        if (currentPerSplitOutputs != null) {
            currentPerSplitOutputs.emitPeriodicWatermark();
        }
        if (currentMainOutput != null) {
            currentMainOutput.emitPeriodicWatermark();
        }
    }

    // ------------------------------------------------------------------------

    private static final class StreamingReaderOutput<T> extends SourceOutputWithWatermarks<T>
            implements ReaderOutput<T> {

        private final SplitLocalOutputs<T> splitLocalOutputs;

        StreamingReaderOutput(
                PushingAsyncDataInput.DataOutput<T> output,
                WatermarkOutput watermarkOutput,
                TimestampAssigner<T> timestampAssigner,
                WatermarkGenerator<T> watermarkGenerator,
                SplitLocalOutputs<T> splitLocalOutputs,
                InternalSourceReaderMetricGroup metricGroup) {

            super(
                    output,
                    watermarkOutput,
                    watermarkOutput,
                    timestampAssigner,
                    watermarkGenerator,
                    metricGroup);
            this.splitLocalOutputs = splitLocalOutputs;
        }

        @Override
        public SourceOutput<T> createOutputForSplit(String splitId) {
            return splitLocalOutputs.createOutputForSplit(splitId);
        }

        @Override
        public void releaseOutputForSplit(String splitId) {
            splitLocalOutputs.releaseOutputForSplit(splitId);
        }
    }

    // ------------------------------------------------------------------------

    /**
     * A holder and factory for split-local {@link SourceOutput}s. The split-local outputs maintain
     * local watermark generators with their own state, to facilitate per-split watermarking logic.
     *
     * @param <T> The type of the emitted records.
     */
    private static final class SplitLocalOutputs<T> {

        private final WatermarkOutputMultiplexer watermarkMultiplexer;
        private final Map<String, SourceOutputWithWatermarks<T>> localOutputs;
        private final PushingAsyncDataInput.DataOutput<T> recordOutput;
        private final TimestampAssigner<T> timestampAssigner;
        private final WatermarkGeneratorSupplier<T> watermarksFactory;
        private final WatermarkGeneratorSupplier.Context watermarkContext;
        private final WatermarkUpdateListener watermarkUpdateListener;
        private final InternalSourceReaderMetricGroup metricGroup;

        private SplitLocalOutputs(
                PushingAsyncDataInput.DataOutput<T> recordOutput,
                WatermarkOutput watermarkOutput,
                WatermarkUpdateListener watermarkUpdateListener,
                TimestampAssigner<T> timestampAssigner,
                WatermarkGeneratorSupplier<T> watermarksFactory,
                WatermarkGeneratorSupplier.Context watermarkContext,
                InternalSourceReaderMetricGroup metricGroup) {

            this.recordOutput = recordOutput;
            this.timestampAssigner = timestampAssigner;
            this.watermarksFactory = watermarksFactory;
            this.watermarkContext = watermarkContext;
            this.watermarkUpdateListener = watermarkUpdateListener;
            this.metricGroup = metricGroup;

            this.watermarkMultiplexer = new WatermarkOutputMultiplexer(watermarkOutput);
            this.localOutputs =
                    new LinkedHashMap<>(); // we use a LinkedHashMap because it iterates faster
        }

        SourceOutput<T> createOutputForSplit(String splitId) {
            final SourceOutputWithWatermarks<T> previous = localOutputs.get(splitId);
            if (previous != null) {
                return previous;
            }

            watermarkMultiplexer.registerNewOutput(
                    splitId,
                    watermark ->
                            watermarkUpdateListener.updateCurrentSplitWatermark(
                                    splitId, watermark));
            final WatermarkOutput onEventOutput = watermarkMultiplexer.getImmediateOutput(splitId);
            final WatermarkOutput periodicOutput = watermarkMultiplexer.getDeferredOutput(splitId);

            final WatermarkGenerator<T> watermarks =
                    watermarksFactory.createWatermarkGenerator(watermarkContext);

            final SourceOutputWithWatermarks<T> localOutput =
                    SourceOutputWithWatermarks.createWithSeparateOutputs(
                            recordOutput,
                            onEventOutput,
                            periodicOutput,
                            timestampAssigner,
                            watermarks,
                            metricGroup);

            localOutputs.put(splitId, localOutput);
            return localOutput;
        }

        void releaseOutputForSplit(String splitId) {
            localOutputs.remove(splitId);
            watermarkMultiplexer.unregisterOutput(splitId);
        }

        void emitPeriodicWatermark() {
            // The call in the loop only records the next watermark candidate for each local output.
            // The call to 'watermarkMultiplexer.onPeriodicEmit()' actually merges the watermarks.
            // That way, we save inefficient repeated merging of (partially outdated) watermarks
            // before
            // all local generators have emitted their candidates.
            for (SourceOutputWithWatermarks<?> output : localOutputs.values()) {
                output.emitPeriodicWatermark();
            }
            watermarkMultiplexer.onPeriodicEmit();
        }
    }

    /**
     * A helper class for managing idleness status of the underlying output.
     *
     * <p>This class tracks the idleness status of main and split-local output, and only marks the
     * underlying output as idle if both main and per-split output are idle.
     *
     * <p>The reason of adding this manager is that the implementation of source reader might only
     * use one of main or split-local output for emitting records and watermarks, and we could avoid
     * watermark generator on the vacant output keep marking the underlying output as idle.
     */
    private static class IdlenessManager {
        private final WatermarkOutput underlyingOutput;
        private final IdlenessAwareWatermarkOutput splitLocalOutput;
        private final IdlenessAwareWatermarkOutput mainOutput;

        IdlenessManager(WatermarkOutput underlyingOutput) {
            this.underlyingOutput = underlyingOutput;
            this.splitLocalOutput = new IdlenessAwareWatermarkOutput(underlyingOutput);
            this.mainOutput = new IdlenessAwareWatermarkOutput(underlyingOutput);
        }

        IdlenessAwareWatermarkOutput getSplitLocalOutput() {
            return splitLocalOutput;
        }

        IdlenessAwareWatermarkOutput getMainOutput() {
            return mainOutput;
        }

        void maybeMarkUnderlyingOutputAsIdle() {
            if (splitLocalOutput.isIdle && mainOutput.isIdle) {
                underlyingOutput.markIdle();
            }
        }

        private class IdlenessAwareWatermarkOutput implements WatermarkOutput {
            private final WatermarkOutput underlyingOutput;
            private boolean isIdle = true;

            private IdlenessAwareWatermarkOutput(WatermarkOutput underlyingOutput) {
                this.underlyingOutput = underlyingOutput;
            }

            @Override
            public void emitWatermark(Watermark watermark) {
                underlyingOutput.emitWatermark(watermark);
                isIdle = false;
            }

            @Override
            public void markIdle() {
                isIdle = true;
                maybeMarkUnderlyingOutputAsIdle();
            }

            @Override
            public void markActive() {
                isIdle = false;
                underlyingOutput.markActive();
            }
        }
    }
}
