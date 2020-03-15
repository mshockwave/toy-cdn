package uci.edu.cs230.toy_cdn;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class AnalysisEngineService extends Thread {
    private final static Logger LOG = LogManager.getLogger(AnalysisEngineService.class);

    private ZContext mInternalCtx;

    private ZMQ.Socket mSocketInternal;

    /**
     * only used for testing
     * */
    boolean InitializeOnly = false;

    public AnalysisEngineService(ZContext ipcContext) {
        mInternalCtx = ipcContext;
    }

    private void init() {
        LOG.info("Initializing AnalysisEngine...");

        mSocketInternal = mInternalCtx.createSocket(SocketType.PUSH);
        mSocketInternal.bind("inproc://ae");
        LOG.debug("Set up AnalysisEngineService");
        var syncInternal = mInternalCtx.createSocket(SocketType.PAIR);
        syncInternal.connect("inproc://sync-coordinator-ae");
        syncInternal.send("READY", 0);
        syncInternal.close();
    }

    @Override
    public void run() {
        init();
        if(InitializeOnly) return;
    }
}
