package bench.tpcc.server;

import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.transport.TTransport;
import thrift.DBServiceHandler;
import thrift.server.DBService;
import thrift.tpcc.TpccService;

/**
 * Created by gomes on 01/05/15.
 */
public class TpccProcessorFactory extends TProcessorFactory {

    public TpccProcessorFactory() {
        super(null);
    }

    @Override
    public TProcessor getProcessor(TTransport trans) {
        return new TpccService.Processor<TpccServiceHandler>(new TpccServiceHandler());
    }

    @Override
    public boolean isAsyncProcessor() {
        return false;
    }
}
