package org.apache.flink.table.planner.functions.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperator;

public class SqlSlideTableFunction extends SqlWindowTableFunction {

    /** The window is allow lateness or not. */
    protected static final String PARAM_ALLOWED_LATENESS = "ALLOWED_LATENESS";

    public SqlSlideTableFunction() {
        super("SLIDE", new OperandMetadataImpl());
    }

    /** Operand type checker for SLIDE. */
    private static class OperandMetadataImpl extends AbstractOperandMetadata {
        OperandMetadataImpl() {
            super(
                    ImmutableList.of(
                            PARAM_DATA,
                            PARAM_TIMECOL,
                            PARAM_SIZE,
                            PARAM_ALLOWED_LATENESS),
                    2);
        }

        @Override
        public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
            // There should only be three operands, and number of operands are checked before
            // this call.
            if (!checkTableAndDescriptorOperands(callBinding, 1)) {
                return throwValidationSignatureErrorOrReturnFalse(callBinding, throwOnFailure);
            }

            SqlValidator validator = callBinding.getValidator();
            RelDataType intervalType = validator.getValidatedNodeType(callBinding.operand(2));
            if (!SqlTypeUtil.isInterval(intervalType)) {
                return throwValidationSignatureErrorOrReturnFalse(callBinding, throwOnFailure);
            }

            int size = callBinding.getOperandCount();
            if (size == 4) {
                RelDataType booleanType = validator.getValidatedNodeType(callBinding.operand(3));
                if (!SqlTypeUtil.isBoolean(booleanType)) {
                    return throwValidationSignatureErrorOrReturnFalse(callBinding, throwOnFailure);
                }
            }

            // check time attribute
            return throwExceptionOrReturnFalse(
                    checkTimeColumnDescriptorOperand(callBinding, 1), throwOnFailure);
        }

        @Override
        public String getAllowedSignatures(SqlOperator op, String opName) {
            return opName + "(TABLE table_name, DESCRIPTOR(timecol), datetime interval[, boolean])";
        }
    }
}
