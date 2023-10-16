/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.functions.sql;

import org.apache.calcite.rel.type.RelDataType;

import org.apache.calcite.sql.type.SqlTypeUtil;

import org.apache.calcite.sql.validate.SqlValidator;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

/**
 * SqlHopV2TableFunction implements an operator for hoppingV2.
 *
 * <p>It allows four parameters:
 *
 * <ol>
 *   <li>a table
 *   <li>a descriptor to provide a time attribute column name from the input table
 *   <li>an interval parameter to specify the length of window shifting
 *   <li>an interval parameter to specify the length of window size
 *   <li>an boolean parameter to specify allow lateness or not
 * </ol>
 */
public class SqlHopV2TableFunction extends SqlWindowTableFunction {

    /** The window is allow lateness or not. */
    protected static final String PARAM_ALLOWED_LATENESS = "ALLOWED_LATENESS";

    public SqlHopV2TableFunction() {
        super("HOPV2", new OperandMetadataImpl());
    }

    /** Operand type checker for HOP. */
    private static class OperandMetadataImpl extends AbstractOperandMetadata {
        OperandMetadataImpl() {
            super(
                    ImmutableList.of(
                            PARAM_DATA, PARAM_TIMECOL, PARAM_SLIDE, PARAM_SIZE, PARAM_ALLOWED_LATENESS),
                    4);
        }

        @Override
        public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
            if (!checkTableAndDescriptorOperands(callBinding, 1)) {
                return throwValidationSignatureErrorOrReturnFalse(callBinding, throwOnFailure);
            }
            SqlValidator validator = callBinding.getValidator();
            if (!SqlTypeUtil.isInterval(validator.getValidatedNodeType(callBinding.operand(2)))) {
                return throwValidationSignatureErrorOrReturnFalse(callBinding, throwOnFailure);
            }
            if (!SqlTypeUtil.isInterval(validator.getValidatedNodeType(callBinding.operand(3)))) {
                return throwValidationSignatureErrorOrReturnFalse(callBinding, throwOnFailure);
            }
            int size = callBinding.getOperandCount();
            if (size == 5) {
                RelDataType booleanType = validator.getValidatedNodeType(callBinding.operand(4));
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
            return opName
                    + "(TABLE table_name, DESCRIPTOR(timecol), "
                    + "datetime interval, datetime interval[, boolean])";
        }
    }
}
