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
import org.apache.log4j.Logger;

import javax.xml.crypto.Data;
import java.io.IOException;
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

    protected static final Logger log = Logger.getLogger(SingleThreadedProcessing.class);
    /**
     * Constructor
     *
     * @param maximumNumberOfLines set maximumNumberOfLines
     */
    public SingleThreadedProcessing(final long maximumNumberOfLines) {
        //userDataOutput = dataConsumer;
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
    public void processOutput(Map<String, String> map, AtomicBoolean searchExitFlag) throws IOException {

        long linesLong = lines.longValue();

        while (!hardExitFlag.get() && maxNumberOfLines != -1 && linesLong <= maxNumberOfLines) {

            System.out.println("Flag Value: " + hardExitFlag.get() + "and maximum number of line: " + maxNumberOfLines);

            linesLong += 1;
            //System.out.println("LinesLong: " + linesLong);

            if(map != null) {
                userDataOutput.consume(map);
            }
        }
        //hardExitFlag.set(true);
    }
}




/*
        long lines = 0;
        while (!hardExitFlag.get()) {
            //Map<String, String> row = queue.poll();
            if (map != null) {
                lines += userDataOutput.consume(map);
            } else {
                if (searchExitFlag.get()) {
                    break;
                } else if (hardExitFlag.get()) {
                    break;
                }
            }

            if (maxNumberOfLines != -1 && lines >= maxNumberOfLines) {
                break;
            }
        }

        if (searchExitFlag.get()) {
            log.info("Exiting, search exit flag is true");
        }

        if (hardExitFlag.get()) {
            log.info("Exiting, consumer exit flag is true");
        }

        searchExitFlag.set(true);
        hardExitFlag.set(true);

    }
*/