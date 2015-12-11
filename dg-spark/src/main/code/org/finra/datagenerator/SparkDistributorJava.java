package org.finra.datagenerator;
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

import com.javafx.tools.doclets.formats.html.SourceToHTMLConverter;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.finra.datagenerator.consumer.DataConsumer;
import org.finra.datagenerator.consumer.DataPipe;
import org.finra.datagenerator.consumer.EquivalenceClassTransformer;
import org.finra.datagenerator.distributor.SearchDistributor;
import org.finra.datagenerator.distributor.multithreaded.SingleThreadedProcessing;
import org.finra.datagenerator.engine.Frontier;
import org.finra.datagenerator.samples.transformer.SampleMachineTransformer;
import org.finra.datagenerator.writer.DefaultWriter;

import javax.xml.bind.SchemaOutputResolver;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Uses a multithreaded approach to process Frontiers in parallel. 
 *
 * Created by Brijesh
 */
public class SparkDistributorJava implements SearchDistributor, Serializable {

    /**
     * Logger
     */
    protected static final Logger log = Logger.getLogger(SparkDistributorJava.class);

    private int threadCount = 1;
    private final Queue<Map<String, String>> queue = new ConcurrentLinkedQueue<>();
    private static DataConsumer dataConsumer = new DataConsumer();
    private final AtomicBoolean searchExitFlag = new AtomicBoolean(false);
    private final AtomicBoolean hardExitFlag = new AtomicBoolean(false);
    private long maxNumberOfLines = -1;
    public String masterURL;

    public SparkDistributorJava(String masterURL) {
        this.masterURL = masterURL;
    }
    /**
     * Sets the maximum number of lines to generate
     *
     * @param numberOfLines a long containing the maximum number of lines to
     *                      generate
     * @return a reference to the current DefaultDistributor
     */
    public SparkDistributorJava setMaxNumberOfLines(long numberOfLines) {
        this.maxNumberOfLines = numberOfLines;
        return this;
    }

    /**
     * Sets the number of threads to use
     *
     * @param threadCount an int containing the thread count
     * @return a reference to the current DefaultDistributor
     */
    public SparkDistributorJava setThreadCount(int threadCount) {
        this.threadCount = threadCount;
        return this;
    }

    @Override
    public SparkDistributorJava setDataConsumer(DataConsumer dataConsumer) {
        this.dataConsumer = dataConsumer;
        dataConsumer.setExitFlag(hardExitFlag);
        return this;
    }

    @Override
    public void distribute(final List<Frontier> frontierList) {

        final SingleThreadedProcessing singleThreadedProcessing = new SingleThreadedProcessing(maxNumberOfLines);

        final SparkConf conf = new SparkConf().setAppName("dg-spark").setMaster(masterURL);

        JavaSparkContext sc = new JavaSparkContext(conf);

        System.out.println("Frontier List size: " + frontierList.size());

        final String[] outTemplate = new String[]{"var_1_1", "var_1_2", "var_1_3", "var_1_4", "var_1_5", "var_1_6",     //Added by Shraddha Patel
                                            "var_2_1", "var_2_2", "var_2_3", "var_2_4", "var_2_5", "var_2_6"};

        JavaRDD<DataPipe> mapJavaRDD = sc.parallelize(frontierList).map(new Function<Frontier, DataPipe>() {        // Updated by Shraddha Patel

            DataPipe dataPipe;
            @Override
            public DataPipe call(Frontier frontier) throws Exception {                  // All comments by Shraddha Patel

            //    try(OutputStream out = new FileOutputStream("./dg-spark/out/out" + Math.random() +".txt", true)) {

              //      DefaultWriter dw = new DefaultWriter(out, outTemplate);

                    dataConsumer.addDataTransformer(new SampleMachineTransformer());

                    dataConsumer.addDataTransformer(new EquivalenceClassTransformer());

              //      dataConsumer.addDataWriter(dw);

                    singleThreadedProcessing.setDataConsumer(dataConsumer);

                    setDataConsumer(dataConsumer);

                    dataPipe = frontier.searchForScenarios(singleThreadedProcessing, searchExitFlag);

                    dataPipe.getPipeDelimited(outTemplate);

          //      }
                return dataPipe;                // All comments by Shraddha Patel
            }
        });

        // You need to change this path to your local machine path
        String path = "/Users/k25469/projects/DGSaveAsTextFile/dg-spark/out/result.txt"; // Changed path

        mapJavaRDD.saveAsTextFile(path);

    }
}