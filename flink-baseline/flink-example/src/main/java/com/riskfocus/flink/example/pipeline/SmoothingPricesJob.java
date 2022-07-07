/*
 * Copyright 2020-2022 Ness USA, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.riskfocus.flink.example.pipeline;

import com.riskfocus.flink.example.pipeline.config.channel.InterestRateChannelFactory;
import com.riskfocus.flink.example.pipeline.config.channel.OptionPriceChannelFactory;
import com.riskfocus.flink.example.pipeline.manager.FlinkJobManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;

import java.time.ZoneId;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
public class SmoothingPricesJob {

    @SuppressWarnings("java:S4823")
    public static void main(String[] args) throws Exception {
        log.info("Time zone is: {}", ZoneId.systemDefault());

        ParameterTool params = ParameterTool.fromArgs(args);
        log.info("Job params: {}", params.toMap());

        FlinkJobManager.builder()
                .params(params)
                .interestRateChannelFactory(new InterestRateChannelFactory())
                .optionPriceChannelFactory(new OptionPriceChannelFactory())
                .build().runJob();

    }
}
