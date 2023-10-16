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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import java.time.Duration;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.TimeUtils.formatWithHighestUnit;

/** Logical representation of a hoppingV2 window specification. */
@JsonTypeName("HoppingV2Window")
public class HoppingV2WindowSpec implements WindowSpec {
    public static final String FIELD_NAME_SIZE = "size";
    public static final String FIELD_NAME_SLIDE = "slide";
    public static final String FIELD_NAME_ALLOWED_LATENESS = "allowedLateness";

    @JsonProperty(FIELD_NAME_SIZE)
    private final Duration size;

    @JsonProperty(FIELD_NAME_SLIDE)
    private final Duration slide;

    @JsonProperty(FIELD_NAME_ALLOWED_LATENESS)
    private final Boolean allowedLateness;

    @JsonCreator
    public HoppingV2WindowSpec(
            @JsonProperty(FIELD_NAME_SIZE) Duration size,
            @JsonProperty(FIELD_NAME_SLIDE) Duration slide,
            @JsonProperty(FIELD_NAME_ALLOWED_LATENESS) Boolean allowedLateness) {
        this.size = checkNotNull(size);
        this.slide = checkNotNull(slide);
        this.allowedLateness = checkNotNull(allowedLateness);
    }

    @Override
    public String toSummaryString(String windowing) {
        return String.format(
                "HOPV2(%s, size=[%s], slide=[%s], allowedLateness=[%s])",
                windowing,
                formatWithHighestUnit(size),
                formatWithHighestUnit(slide),
                allowedLateness);
    }

    public Duration getSize() {
        return size;
    }

    public Duration getSlide() {
        return slide;
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
        HoppingV2WindowSpec that = (HoppingV2WindowSpec) o;
        return size.equals(that.size)
                && slide.equals(that.slide)
                && Objects.equals(allowedLateness, that.allowedLateness);
    }

    @Override
    public int hashCode() {
        return Objects.hash(HoppingV2WindowSpec.class, size, slide, allowedLateness);
    }

    @Override
    public String toString() {
        return String.format(
                "HOPV2(size=[%s], slide=[%s], allowedLateness=[%s])",
                formatWithHighestUnit(size),
                formatWithHighestUnit(slide),
                allowedLateness);
    }
}
