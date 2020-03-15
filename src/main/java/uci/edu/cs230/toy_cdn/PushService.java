package uci.edu.cs230.toy_cdn;

import org.zeromq.ZContext;

/**
 * Push service will push any file instructed by the coordinator
 * */
public class PushService extends Thread {
    private ZContext mInternalCtx;

    public PushService(ZContext ipcContext) {
        mInternalCtx = ipcContext;
    }

    @Override
    public void run() {

    }
}
