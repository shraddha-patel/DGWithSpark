package org.finra.datagenerator.distributor.multithreaded;

import org.apache.log4j.Logger;
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

    protected static final Logger log = Logger.getLogger(DefaultDistributor.class);

    /**
     * Constructor
     *
     * @param dataConsumer set DataConsumer
     * @param maximumNumberOfLines set maximumNumberOfLines
     */
    public SingleThreadedProcessing(DataConsumer dataConsumer, long maximumNumberOfLines) {
        userDataOutput = dataConsumer;
        maxNumberOfLines = maximumNumberOfLines;
    }

    /**
     * Pass the generated maps to consumer for processing
     *
     * @param map
     */
    public void processOutput(Map<String,String> map) throws IOException {

        while(!hardExitFlag.get()) {

            userDataOutput.consume(map);
            long linesLong = lines.getAndIncrement();
            if (maxNumberOfLines != -1 && linesLong >= maxNumberOfLines) {
                break;
            }
        }
        hardExitFlag.set(true);
    }
}
