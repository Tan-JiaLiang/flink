package org.apache.flink.table.runtime.operators.window;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.dataview.PerWindowStateDataViewStore;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.generated.NamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.groupwindow.RowtimeAttribute;
import org.apache.flink.table.runtime.groupwindow.WindowEnd;
import org.apache.flink.table.runtime.groupwindow.WindowStart;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

public class HoppingV2WindowAggregateTableFunction
        extends HoppingV2WindowAggregateTableFunctionOperator.StateBackedSettableKeyedProcessFunction<RowData, RowData, RowData> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG =
            LoggerFactory.getLogger(HoppingV2WindowAggregateTableFunction.class);

    private final long slide;
    private final long size;
    private final ZoneId timezone;
    private final int rowTimeIdx;
    private final GeneratedNamespaceAggsHandleFunction<Long> genAggsHandler;
    private final LogicalType[] accTypes;
    private final LogicalType[] inputFieldTypes;
    private final List<String> windowProperties;
    private transient NamespaceAggsHandleFunction<Long> function;

    // when allow lateness, the late record will be accumulate without emit
    private final boolean allowedLateness;

    // the state which keeps all the data that are not expired.
    // The first element (as the mapState key) of the tuple is the time stamp. Per each time stamp,
    // the second element of tuple is a list that contains the entire data of all the rows belonging
    // to this time stamp.
    private transient MapState<Long, List<RowData>> inputState;

    // the state which keeps all the accumulated data that are not expired.
    // the first element (as the mapState key) of the tuple is the timestamp for slice window start.
    // the second element of tuple is the accumulated data.
    private transient MapState<Long, RowData> perWindowAccState;

    // ------------------------------------------------------------------------
    // Metrics
    // ------------------------------------------------------------------------
    private static final String LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped";
    private transient Counter numLateRecordsDropped;

    @VisibleForTesting
    protected Counter getCounter() {
        return numLateRecordsDropped;
    }

    public HoppingV2WindowAggregateTableFunction(
            long slide,
            long size,
            boolean allowedLateness,
            ZoneId timezone,
            GeneratedNamespaceAggsHandleFunction<Long> genAggsHandler,
            LogicalType[] accTypes,
            LogicalType[] inputFieldTypes,
            int rowTimeIdx,
            List<String> windowProperties) {
        this.slide = slide;
        this.size = size;
        this.allowedLateness = allowedLateness;
        this.timezone = timezone;
        this.rowTimeIdx = rowTimeIdx;
        this.genAggsHandler = genAggsHandler;
        this.accTypes = accTypes;
        this.inputFieldTypes = inputFieldTypes;
        this.windowProperties = windowProperties;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        function = genAggsHandler.newInstance(getRuntimeContext().getUserCodeClassLoader());
        function.open(
                new PerWindowStateDataViewStore(
                        this.keyedStateBackend,
                        LongSerializer.INSTANCE,
                        getRuntimeContext())
        );

        // input element are all binary row as they are came from network
        InternalTypeInfo<RowData> inputType = InternalTypeInfo.ofFields(inputFieldTypes);
        ListTypeInfo<RowData> rowListTypeInfo = new ListTypeInfo<>(inputType);
        MapStateDescriptor<Long, List<RowData>> inputStateDesc =
                new MapStateDescriptor<>(
                        "inputState", Types.LONG, rowListTypeInfo);
        inputState = getRuntimeContext().getMapState(inputStateDesc);

        InternalTypeInfo<RowData> perWindowAccTypeInfo = InternalTypeInfo.ofFields(accTypes);
        MapStateDescriptor<Long, RowData> perWindowAccStateDesc =
                new MapStateDescriptor<>("perWindowState", Types.LONG, perWindowAccTypeInfo);
        perWindowAccState = getRuntimeContext().getMapState(perWindowAccStateDesc);

        // metrics
        this.numLateRecordsDropped =
                getRuntimeContext().getMetricGroup().counter(LATE_ELEMENTS_DROPPED_METRIC_NAME);
    }

    @Override
    public void processElement(
            RowData input,
            Context ctx,
            Collector<RowData> out)
            throws Exception {
        if (input.isNullAt(rowTimeIdx)) {
            throw new RuntimeException(
                    "RowTime field should not be null,"
                            + " please convert it to a non-null long value.");
        }

        // triggering timestamp for trigger calculation
        long triggeringTs = TimeWindowUtil.toUtcTimestampMills(input.getLong(rowTimeIdx), timezone);

        // check if the data is expired
        long currentWatermark = ctx.timerService().currentWatermark();
        if (lateness(triggeringTs, currentWatermark)) {
            numLateRecordsDropped.inc();
            return;
        }

        long windowSlice = getCurrentSlice(triggeringTs);
        if (triggeringTs > currentWatermark) {
            List<RowData> data = inputState.get(triggeringTs);
            if (data == null) {
                data = new ArrayList<>();
                data.add(input);
                inputState.put(triggeringTs, data);
                // register event time timer
                ctx.timerService().registerEventTimeTimer(triggeringTs);
            } else {
                data.add(input);
                inputState.put(triggeringTs, data);
            }
        } else {
            // accumulate the allowedLateness data without emit
            RowData accValue = perWindowAccState.get(windowSlice);
            if (accValue == null) {
                accValue = function.createAccumulators();
            }
            function.setAccumulators(windowSlice, accValue);
            function.accumulate(input);
            perWindowAccState.put(windowSlice, function.getAccumulators());
        }

        // register timer to retract the slice
        ctx.timerService().registerEventTimeTimer(windowSlice + size);
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<RowData> out)
            throws Exception {
        long windowSlice = getCurrentSlice(timestamp);
        long windowStart = getWindowStart(timestamp);
        long windowEnd = getWindowEnd(timestamp);

        // expiration
        long retractWindowSlice = timestamp - size;
        if (perWindowAccState.contains(retractWindowSlice)) {
            // emit before expiration
            emit(retractWindowSlice, timestamp - 1, timestamp, ctx.getCurrentKey(), out);

            function.cleanup(retractWindowSlice);
            perWindowAccState.remove(retractWindowSlice);
            if (perWindowAccState.isEmpty()) {
                // clear all state
                perWindowAccState.clear();
                if (inputState.isEmpty()) {
                    inputState.clear();
                }
            }
        }

        // accumulate
        List<RowData> inputs = inputState.get(timestamp);
        if (!CollectionUtil.isNullOrEmpty(inputs)) {
            RowData accValue = perWindowAccState.get(windowSlice);
            if (accValue == null) {
                accValue = function.createAccumulators();
            }
            function.setAccumulators(windowSlice, accValue);
            for (RowData input : inputs) {
                function.accumulate(input);
            }
            perWindowAccState.put(windowSlice, function.getAccumulators());
            inputState.remove(timestamp);
        }

        // emit
        emit(windowStart, timestamp, windowEnd, ctx.getCurrentKey(), out);
    }

    private boolean lateness(long triggeringTs, long currentWatermark) {
        if (triggeringTs > currentWatermark) {
            return false;
        }

        if (allowedLateness && getCurrentSlice(triggeringTs) > currentWatermark - size) {
            return false;
        }

        return true;
    }

    private long getWindowStart(long timestamp) {
        return getCurrentSlice(timestamp - size + slide);
    }

    private long getWindowEnd(long timestamp) {
        return getWindowStart(timestamp) + size;
    }

    private long getCurrentSlice(long timestamp) {
        return TimeWindow.getWindowStartWithOffset(timestamp, 0, slide);
    }

    private void emit(
            long windowStart,
            long windowTime,
            long windowEnd,
            RowData inputRow,
            Collector<RowData> out) throws Exception {
        // null namespace means use heap data views
        function.setAccumulators(null, function.createAccumulators());

        // merge
        long sliceSize = Math.floorDiv(size, slide);
        for (int i = 0; i < sliceSize; i++) {
            long slice = windowStart + (i * slide);
            RowData value = perWindowAccState.get(slice);
            if (value != null) {
                function.merge(slice, value);
            }
        }

        // emit
        RowData output =
                toOutputRowData(
                        inputRow,
                        function.getValue(windowStart),
                        windowStart,
                        windowTime,
                        windowEnd);
        out.collect(output);
    }

    private RowData toOutputRowData(
            RowData inputRow,
            RowData aggValue,
            long windowStart,
            long windowTime,
            long windowEnd) {
        JoinedRowData output = new JoinedRowData();
        if (windowProperties.isEmpty()) {
            return output.replace(inputRow, aggValue);
        } else {
            GenericRowData windowRowData =
                    new GenericRowData(windowProperties.size());
            for (int i = 0; i < windowProperties.size(); i++) {
                String type = windowProperties.get(i);
                TimestampData field = null;
                if (RowtimeAttribute.class.getSimpleName().equals(type)) {
                    field = TimestampData.fromEpochMillis(windowTime);
                } else if (WindowStart.class.getSimpleName().equals(type)) {
                    field = TimestampData.fromEpochMillis(windowStart);
                } else if (WindowEnd.class.getSimpleName().equals(type)) {
                    field = TimestampData.fromEpochMillis(windowEnd);
                }
                windowRowData.setField(i, field);
            }
            return output.replace(new JoinedRowData(inputRow, aggValue), windowRowData);
        }
    }

    @Override
    public void close() throws Exception {
        if (null != function) {
            function.close();
        }
    }
}
