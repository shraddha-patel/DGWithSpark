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
import org.finra.datagenerator.consumer.DataPipe;
import org.finra.datagenerator.distributor.ProcessingStrategy;
import org.apache.log4j.Logger;
import org.finra.datagenerator.writer.DataWriter;
import org.finra.datagenerator.writer.DefaultWriter;

import javax.xml.crypto.Data;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Brijesh on 9/29/2015.
 */
public class SingleThreadedProcessing implements ProcessingStrategy,Serializable {

    private DataConsumer userDataOutput;
    private long maxNumberOfLines = -1;
    private AtomicLong lines = new AtomicLong(0);
    private AtomicBoolean hardExitFlag = new AtomicBoolean(false);
    private DataPipe dataPipe;

    protected static final Logger log = Logger.getLogger(SingleThreadedProcessing.class);
    /**
     * Constructor
     *
     * @param maximumNumberOfLines set maximumNumberOfLines
     */
    public SingleThreadedProcessing(final long maximumNumberOfLines) {
        maxNumberOfLines = maximumNumberOfLines;
    }

    public SingleThreadedProcessing setDataConsumer(final DataConsumer dataConsumer) {
        this.userDataOutput = dataConsumer;
        dataConsumer.setExitFlag(hardExitFlag);
        return this;
    }

    /**
     * Pass the generated maps to consumer for processing
     *
     * @param map Map of String and String
     * @throws IOException Input Output exception
     */
    public DataPipe processOutput(Map<String, String> map, AtomicBoolean searchExitFlag) throws IOException {       //Updated by Shraddha Patel

        long linesLong = lines.longValue();

        while (!hardExitFlag.get() && maxNumberOfLines != -1 && linesLong <= maxNumberOfLines) {

            System.out.println("Flag Value: " + hardExitFlag.get() + " and maximum number of line: " + maxNumberOfLines);

            linesLong += 1;

            if(map != null) {
                dataPipe = userDataOutput.consumeMap(map);
            }
        }
        return dataPipe;
    }
}