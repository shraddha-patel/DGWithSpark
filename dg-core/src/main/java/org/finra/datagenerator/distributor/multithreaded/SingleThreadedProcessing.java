/*
 * Copyright 2014 DataGenerator Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finra.datagenerator.distributor.multithreaded;

import org.finra.datagenerator.consumer.DataConsumer;
import org.finra.datagenerator.distributor.ProcessingStrategy;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Brijesh on 9/29/2015.
 */
public class SingleThreadedProcessing implements ProcessingStrategy {

    private DataConsumer userDataOutput;
    private long maxNumberOfLines = -1;
    private AtomicLong lines = new AtomicLong(1);
    private AtomicBoolean hardExitFlag = new AtomicBoolean(false);

    /**
     * Constructor
     *
     * @param dataConsumer set DataConsumer
     * @param maximumNumberOfLines set maximumNumberOfLines
     */
    public SingleThreadedProcessing(final DataConsumer dataConsumer, final long maximumNumberOfLines) {
        userDataOutput = dataConsumer;
        maxNumberOfLines = maximumNumberOfLines;
    }

    /**
     * Pass the generated maps to consumer for processing
     *
     * @param map Map of String and String
     * @throws IOException Input Output exception
     */
    public void processOutput(Map<String, String> map) throws IOException {

        while (!hardExitFlag.get()) {

            userDataOutput.consume(map);
            long linesLong = lines.getAndIncrement();
            if (maxNumberOfLines != -1 && linesLong >= maxNumberOfLines) {
                break;
            }
        }
        hardExitFlag.set(true);
    }
}
