package org.finra.datagenerator.distributor;

import java.io.IOException;
import java.util.Map;

/**
 * Created by Brijesh on 9/29/2015.
 */
public interface ProcessingStrategy {

    /**
     * Takes the map generated from SCXMLFrontier and pass it
     * to the consumer for data processing
     *
     * @param map
     */
    void processOutput(Map<String,String> map) throws IOException;

}
