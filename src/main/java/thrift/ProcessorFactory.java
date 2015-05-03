package thrift;

import thrift.server.DBService;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.transport.TTransport;

/**
 * Created by gomes on 01/05/15.
 */
public class ProcessorFactory extends TProcessorFactory {

    public ProcessorFactory() {
        super(null);
    }

    @Override
    public TProcessor getProcessor(TTransport trans) {
        return new DBService.Processor<DBServiceHandler>(new DBServiceHandler());
    }

    @Override
    public boolean isAsyncProcessor() {
        return false;
    }
}
