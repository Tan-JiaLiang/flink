package org.apache.flink.table.runtime.operators.window;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.groupwindow.RowtimeAttribute;
import org.apache.flink.table.runtime.groupwindow.WindowEnd;
import org.apache.flink.table.runtime.groupwindow.WindowStart;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

public class SlidingWindowAggregateTableFunctionOperator
        extends KeyedProcessFunction<RowData, RowData, RowData> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG =
            LoggerFactory.getLogger(SlidingWindowAggregateTableFunctionOperator.class);

    private final long size;
    private final ZoneId timezone;
    private final int rowTimeIdx;
    private final GeneratedAggsHandleFunction genAggsHandler;
    private final LogicalType[] accTypes;
    private final LogicalType[] inputFieldTypes;
    private final List<String> windowProperties;

    // when allow lateness, the late record will be accumulate without emit
    private final boolean allowedLateness;

    private transient JoinedRowData output;

    // the state which keeps the last triggering timestamp
    private transient ValueState<Long> lastTriggeringTsState;

    // the state which used to materialize the accumulator for incremental calculation
    private transient ValueState<RowData> accState;

    // the state which keeps all the data that are not expired.
    // The first element (as the mapState key) of the tuple is the time stamp. Per each time stamp,
    // the second element of tuple is a list that contains the entire data of all the rows belonging
    // to this time stamp.
    private transient MapState<Long, List<RowData>> inputState;

    // the state which keeps the timestamp to retract inputState
    // The first element (as the mapState key) of the tuple is the retract timestamp and the second
    // element is the input timestamp
    private transient MapState<Long, Long> retractState;

    private transient AggsHandleFunction function;

    // ------------------------------------------------------------------------
    // Metrics
    // ------------------------------------------------------------------------
    private static final String LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped";
    private transient Counter numLateRecordsDropped;

    @VisibleForTesting
    protected Counter getCounter() {
        return numLateRecordsDropped;
    }

    public SlidingWindowAggregateTableFunctionOperator(
            long size,
            boolean allowedLateness,
            ZoneId timezone,
            GeneratedAggsHandleFunction genAggsHandler,
            LogicalType[] accTypes,
            LogicalType[] inputFieldTypes,
            int rowTimeIdx,
            List<String> windowProperties) {
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
        function.open(new PerKeyStateDataViewStore(getRuntimeContext()));

        output = new JoinedRowData();

        ValueStateDescriptor<Long> lastTriggeringTsDescriptor =
                new ValueStateDescriptor<>("lastTriggeringTsState", Types.LONG);
        lastTriggeringTsState = getRuntimeContext().getState(lastTriggeringTsDescriptor);

        InternalTypeInfo<RowData> accTypeInfo = InternalTypeInfo.ofFields(accTypes);
        ValueStateDescriptor<RowData> accStateDesc =
                new ValueStateDescriptor<>("accState", accTypeInfo);
        accState = getRuntimeContext().getState(accStateDesc);

        // input element are all binary row as they are came from network
        InternalTypeInfo<RowData> inputType = InternalTypeInfo.ofFields(inputFieldTypes);
        ListTypeInfo<RowData> rowListTypeInfo = new ListTypeInfo<>(inputType);
        MapStateDescriptor<Long, List<RowData>> inputStateDesc =
                new MapStateDescriptor<>(
                        "inputState", Types.LONG, rowListTypeInfo);
        inputState = getRuntimeContext().getMapState(inputStateDesc);

        MapStateDescriptor<Long, Long> retractStateDesc =
                new MapStateDescriptor<>(
                        "retractState", Types.LONG, Types.LONG);
        retractState = getRuntimeContext().getMapState(retractStateDesc);

        // metrics
        this.numLateRecordsDropped =
                getRuntimeContext().getMetricGroup().counter(LATE_ELEMENTS_DROPPED_METRIC_NAME);
    }

    @Override
    public void processElement(
            RowData input,
            KeyedProcessFunction<RowData, RowData, RowData>.Context ctx,
            Collector<RowData> out)
            throws Exception {
        if (input.isNullAt(rowTimeIdx)) {
            throw new RuntimeException(
                    "RowTime field should not be null,"
                            + " please convert it to a non-null long value.");
        }

        // triggering timestamp for trigger calculation
        long aLong = input.getLong(rowTimeIdx);
        long triggeringTs = TimeWindowUtil.toUtcTimestampMills(aLong, timezone);

        Long lastTriggeringTs = lastTriggeringTsState.value();
        if (lastTriggeringTs == null) {
            lastTriggeringTs = 0L;
        }

        // check if the data is expired
        if (lateness(triggeringTs, lastTriggeringTs)) {
            numLateRecordsDropped.inc();
            return;
        }

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

            if (triggeringTs <= lastTriggeringTs) {
                // accumulate the allowedLateness data without emit
                function.setAccumulators(
                        accState.value() == null ?
                                function.createAccumulators() :
                                accState.value()
                );
                function.accumulate(input);
                accState.update(function.getAccumulators());
            }
        }

        // calculate timestamp to retract data
        long retractTs = triggeringTs + size;
        ctx.timerService().registerEventTimeTimer(retractTs);
        retractState.put(retractTs, triggeringTs);
    }

    @Override
    public void onTimer(
            long timestamp,
            KeyedProcessFunction<RowData, RowData, RowData>.OnTimerContext ctx,
            Collector<RowData> out)
            throws Exception {
        // initialize when first run or failover recovery per key
        function.setAccumulators(
                accState.value() == null ?
                        function.createAccumulators() :
                        accState.value()
        );

        // retraction
        Long triggerTs = retractState.get(timestamp);
        if (triggerTs != null) {
            List<RowData> retracts = inputState.get(triggerTs);
            if (retracts != null) {
                for (RowData retractRow : retracts) {
                    function.retract(retractRow);
                }
            } else {
                // Does not retract values which are outside of window if the state is
                // cleared already.
                LOG.warn(
                        "The state is cleared because of state ttl. "
                                + "This will result in incorrect result. "
                                + "You can increase the state ttl to avoid this.");
            }

            retractState.remove(timestamp);
            inputState.remove(triggerTs);
        }

        // accumulation
        List<RowData> inputs = inputState.get(timestamp);
        if (inputs != null) {
            for (RowData inputRow : inputs) {
                function.accumulate(inputRow);
            }
        }

        // emit inputs
        RowData aggValue = function.getValue();
        RowData output = toOutputRowData(ctx.getCurrentKey(), aggValue, timestamp);
        out.collect(output);

        // update the trigger timestamp
        lastTriggeringTsState.update(timestamp);

        if (inputState.isEmpty()) {
            // clear all state
            inputState.clear();
            accState.clear();
            lastTriggeringTsState.clear();
            retractState.clear();
            function.cleanup();
        } else {
            // materialize the accumulator for incremental calculation
            accState.update(function.getAccumulators());
        }
    }

    private boolean lateness(long triggeringTs, long lastTriggeringTs) {
        if (triggeringTs > lastTriggeringTs) {
            return false;
        }

        if (allowedLateness && triggeringTs > lastTriggeringTs - size) {
            return false;
        }

        return true;
    }

    private RowData toOutputRowData(RowData inputRow, RowData aggValue, long timestamp) {
        if (windowProperties.isEmpty()) {
            return output.replace(inputRow, aggValue);
        } else {
            GenericRowData windowRowData =
                    new GenericRowData(windowProperties.size());
            for (int i = 0; i < windowProperties.size(); i++) {
                String type = windowProperties.get(i);
                windowRowData.setField(i, this.toTimestampData(type, timestamp));
            }
            return output.replace(new JoinedRowData(inputRow, aggValue), windowRowData);
        }
    }

    private TimestampData toTimestampData(String type, long timestamp) {
        if (RowtimeAttribute.class.getSimpleName().equals(type)) {
            return TimestampData.fromEpochMillis(timestamp - 1);
        } else if (WindowStart.class.getSimpleName().equals(type)) {
            return TimestampData.fromEpochMillis(timestamp - size);
        } else if (WindowEnd.class.getSimpleName().equals(type)) {
            return TimestampData.fromEpochMillis(timestamp);
        } else {
            return null;
        }
    }

    @Override
    public void close() throws Exception {
        if (null != function) {
            function.close();
        }
    }
}
