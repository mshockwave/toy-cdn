package uci.edu.cs230.toy_cdn;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class AbstractMockAnalysisService extends Thread {
    protected static final Logger LOG = LogManager.getLogger(AbstractMockAnalysisService.class);

    protected ZContext mInternalCtx;

    protected ZMQ.Socket mSocketInternal;

    protected AbstractMockAnalysisService(ZContext ipcContext) {
        mInternalCtx = ipcContext;
    }

    protected void init() {
        LOG.info("Initializing MockAnalysisService...");

        mSocketInternal = mInternalCtx.createSocket(SocketType.PUSH);
        mSocketInternal.bind(Common.EP_INT_ANALYSIS_SERVICE);
        LOG.info("Setup MockAnalysisService");
        var syncInternal = mInternalCtx.createSocket(SocketType.PAIR);
        syncInternal.connect(Common.EP_INT_SYNC_COORDINATOR_AE);
        syncInternal.send("READY", 0);
        syncInternal.close();
    }
}