package org.apache.flink.table.runtime.operators.window;

import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.table.data.RowData;

public class HoppingV2WindowAggregateTableFunctionOperator<K, IN, OUT>
        extends KeyedProcessOperator<K, IN, OUT> {
    private static final long serialVersionUID = 1L;

    public HoppingV2WindowAggregateTableFunctionOperator(
            KeyedProcessFunction<K, IN, OUT> function) {
        super(function);
    }

    @Override
    public void open() throws Exception {
        if (userFunction instanceof StateBackedSettableKeyedProcessFunction) {
            ((StateBackedSettableKeyedProcessFunction<K, IN, OUT>) userFunction)
                    .setKeyedBackend(getKeyedStateBackend());
        }
        super.open();
    }

    abstract static class StateBackedSettableKeyedProcessFunction<K, IN, OUT>
            extends KeyedProcessFunction<K, IN, OUT> {

        protected KeyedStateBackend<RowData> keyedStateBackend;

        void setKeyedBackend(KeyedStateBackend<RowData> keyedStateBackend) {
            this.keyedStateBackend = keyedStateBackend;
        }
    }
}
