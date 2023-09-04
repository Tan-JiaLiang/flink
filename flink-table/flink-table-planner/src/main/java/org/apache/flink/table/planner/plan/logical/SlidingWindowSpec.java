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

package org.apache.flink.table.planner.plan.logical;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.TimeUtils.formatWithHighestUnit;

/** Logical representation of a sliding window specification. */
@JsonTypeName("SlidingWindow")
public class SlidingWindowSpec implements WindowSpec {
    public static final String FIELD_NAME_SIZE = "size";
    public static final String ALLOWED_LATENESS = "allowedLateness";

    @JsonProperty(FIELD_NAME_SIZE)
    private final Duration size;

    @JsonProperty(ALLOWED_LATENESS)
    private final Boolean allowedLateness;

    @JsonCreator
    public SlidingWindowSpec(@JsonProperty(FIELD_NAME_SIZE) Duration size,
                             @JsonProperty(ALLOWED_LATENESS) Boolean allowedLateness) {
        this.size = checkNotNull(size);
        this.allowedLateness = Boolean.TRUE.equals(allowedLateness);
    }

    @Override
    public String toSummaryString(String windowing) {
        return String.format("SLIDE(%s, size=[%s], allowedLateness=[%s])",
                windowing, formatWithHighestUnit(size), allowedLateness);
    }

    public Duration getSize() {
        return size;
    }

    public Boolean getAllowedLateness() {
        return allowedLateness;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SlidingWindowSpec that = (SlidingWindowSpec) o;
        return size.equals(that.size) && allowedLateness.equals(that.allowedLateness);
    }

    @Override
    public int hashCode() {
        return Objects.hash(SlidingWindowSpec.class, size, allowedLateness);
    }

    @Override
    public String toString() {
        return String.format("SLIDE(size=[%s], allowedLateness=[%s])",
                formatWithHighestUnit(size), allowedLateness);
    }
}
