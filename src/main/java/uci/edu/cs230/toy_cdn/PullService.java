package uci.edu.cs230.toy_cdn;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

/**
 * Pull service will notify coordinator of any file it received
 * */
public class PullService extends Thread {
    private final static Logger LOG = LogManager.getLogger(PullService.class);

    private ZContext mInternalCtx;

    private ZMQ.Socket mSocketInternal;

    /**
     * only used for testing
     * */
    boolean InitializeOnly = false;

    public PullService(ZContext ipcContext) {
        mInternalCtx = ipcContext;
    }

    private void init() {
        LOG.info("Initializing PullService...");

        mSocketInternal = mInternalCtx.createSocket(SocketType.PUSH);
        mSocketInternal.bind("inproc://pullservice");
        LOG.debug("Set up PullService");
        var syncInternal = mInternalCtx.createSocket(SocketType.PAIR);
        syncInternal.connect("inproc://sync-coordinator-pull");
        syncInternal.send("READY", 0);
        syncInternal.close();
    }

    @Override
    public void run() {
        init();
        if(InitializeOnly) return;
    }
}
