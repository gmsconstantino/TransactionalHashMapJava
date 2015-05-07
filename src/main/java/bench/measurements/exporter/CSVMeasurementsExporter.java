/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */
package bench.measurements.exporter;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Properties;

/**
 * Export measurements into a machine readable JSON file.
 */
public class CSVMeasurementsExporter implements MeasurementsExporter
{
    String keys[] = {"workloads", "recordcount", "operationcount", "threads", "distribution",
            "transactiontype", "run", "runtime", "tx_throughput","n_tx","avg_tx","min_tx","max_tx","Return=0",
            "Return=-1","Return=-2","Return=-3",
            "n_inserts", "avg_inserts", "min_inserts", "max_inserts", "n_reads", "avg_reads", "min_reads",
            "max_reads", "n_updates", "avg_updates", "min_updates", "max_updates",
            "n_begins","avg_begins", "min_begins", "max_begins",
            "n_commit", "avg_commit", "min_commit", "max_commit"};
    HashMap<String, String> data;

    public CSVMeasurementsExporter(OutputStream os, Properties props) throws IOException
    {
        os.close();
        data = new HashMap<String, String>();

        data.put("exportfile", props.getProperty("exportfile"));

        data.put("workloads", props.getProperty("nameworkload","test"));
        data.put("recordcount", props.getProperty("recordcount","-1"));
        data.put("operationcount", props.getProperty("operationcount","-1"));
        data.put("threads", props.getProperty("threadcount","-1"));
        data.put("distribution", props.getProperty("requestdistribution"));
        data.put("transactiontype", props.getProperty("transaction.type","TWOPL"));

        if(props.getProperty("op","load").equals("run"))
            data.put("run", "1");
        else
            data.put("run", "0");
    }

    public void write(String metric, String measurement, int i) throws IOException
    {
        writeMeasurement(metric, measurement, i);
    }

    public void write(String metric, String measurement, double d) throws IOException
    {
        writeMeasurement(metric, measurement, d);
    }

    private void writeMeasurement(String metric, String measurement, double d) {
        if (metric.equals("OVERALL")){
            if (measurement.equals("RunTime(ms)")){
                data.put("runtime", d+"");
            } else if (measurement.equals("Throughput(ops/sec)")){
                data.put("throughput", d+"");
            }
        } else if (metric.equals("INSERT")){
            if (measurement.equals("Operations")){
                data.put("n_inserts", d+"");
            } else if (measurement.equals("AverageLatency(us)")){
                data.put("avg_inserts", d+"");
            } else if (measurement.equals("MinLatency(us)")){
                data.put("min_inserts", d+"");
            } else if (measurement.equals("MaxLatency(us)")){
                data.put("max_inserts", d+"");
            }
        } else if (metric.equals("READ")){
            if (measurement.equals("Operations")){
                data.put("n_reads", d+"");
            } else if (measurement.equals("AverageLatency(us)")){
                data.put("avg_reads", d+"");
            } else if (measurement.equals("MinLatency(us)")){
                data.put("min_reads", d+"");
            } else if (measurement.equals("MaxLatency(us)")){
                data.put("max_reads", d+"");
            }
        } else if (metric.equals("UPDATE")){
            if (measurement.equals("Operations")){
                data.put("n_updates", d+"");
            } else if (measurement.equals("AverageLatency(us)")){
                data.put("avg_updates", d+"");
            } else if (measurement.equals("MinLatency(us)")){
                data.put("min_updates", d+"");
            } else if (measurement.equals("MaxLatency(us)")){
                data.put("max_updates", d+"");
            }
        } else if (metric.equals("BEGIN")){
            if (measurement.equals("Operations")){
                data.put("n_begins", d+"");
            } else if (measurement.equals("AverageLatency(us)")){
                data.put("avg_begins", d+"");
            } else if (measurement.equals("MinLatency(us)")){
                data.put("min_begins", d+"");
            } else if (measurement.equals("MaxLatency(us)")){
                data.put("max_begins", d+"");
            }
        } else if (metric.equals("Tx")){
            if (measurement.equals("Operations")){
                data.put("n_tx", d+"");
            } else if (measurement.equals("AverageLatency(us)")){
                data.put("avg_tx", d+"");
            } else if (measurement.equals("MinLatency(us)")){
                data.put("min_tx", d+"");
            } else if (measurement.equals("MaxLatency(us)")){
                data.put("max_tx", d+"");
            } else if (measurement.startsWith("Return=")){
                data.put(measurement, d+"");
            }
        } else if (metric.equals("COMMIT")){
            if (measurement.equals("Operations")){
                data.put("n_commit", d+"");
            } else if (measurement.equals("AverageLatency(us)")){
                data.put("avg_commit", d+"");
            } else if (measurement.equals("MinLatency(us)")){
                data.put("min_commit", d+"");
            } else if (measurement.equals("MaxLatency(us)")){
                data.put("max_commit", d+"");
            }
        }
    }

    @Override
    public void flush() throws IOException {

        if (data.get("run").equals("1")) {
            double txThroughput = 1000.0 * (Double.parseDouble(data.get("n_tx"))) / (Double.parseDouble(data.get("runtime")));
            data.put("tx_throughput", txThroughput + "");
        }

        String values = "";
        String v;
        int t = keys.length-1;
        for (String key : keys){
            v = data.get(key);
            v = (v==null)?" ":v;
            values += v + (t>0?",":"\n");
            t--;
        }

        String exportFile = data.get("exportfile");
        File file = new File(exportFile);
        FileUtils.writeStringToFile(file, values, true);

//        FileWriter out = new FileWriter(exportFile, true);
//        out.write(values);
//        out.close();
    }

    public void writeHeader() throws IOException {
        String exportFile = data.get("exportfile");
        File file = new File(exportFile);
        String header = "";
        int t = keys.length-1;
        for (String key : keys){
            header += key + (t>0?",":"\n");
            t--;
        }
        FileUtils.writeStringToFile(file, header, true);
    }

    public void close() throws IOException
    {
    }

}
