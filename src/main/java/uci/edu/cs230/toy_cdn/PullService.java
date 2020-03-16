package uci.edu.cs230.toy_cdn;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import uci.edu.cs230.toy_cdn.fbs.Subscription;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Pull service will notify coordinator of any file it received
 * */
public class PullService extends Thread {
    private final static Logger LOG = LogManager.getLogger(PullService.class);

    private ZContext mCtx;
    private ZMQ.Poller mPoller;

    private ZMQ.Socket mSocketInternalLink, mSocketControl;
    private long mSelfId;

    private List<ZMQ.Socket> mSocketSubscriptions;

    /**
     * only used for testing
     * */
    boolean InitializeOnly = false;

    public PullService(ZContext ipcContext, long selfId) {
        mCtx = ipcContext;
        mSelfId = selfId;
        mSocketSubscriptions = new ArrayList<>();
    }

    private void init() {
        LOG.info("Initializing PullService...");

        mSocketInternalLink = mCtx.createSocket(SocketType.PUSH);
        mSocketInternalLink.bind(Common.EP_INT_PULL_SERVICE);
        mSocketControl = mCtx.createSocket(SocketType.PAIR);
        mSocketControl.bind(Common.EP_INT_PULL_CONTROL);
        LOG.debug("Set up PullService");
        var syncInternal = mCtx.createSocket(SocketType.PAIR);
        syncInternal.connect(Common.EP_INT_SYNC_COORDINATOR_PULL);
        syncInternal.send("READY", 0);
        syncInternal.close();
    }

    void handlePullControl(ZMsg recvMsg) {
        if(recvMsg.size() < 1) {
            LOG.error("Empty receive message");
            return;
        }

        var action = recvMsg.pop().getString(ZMQ.CHARSET);
        if(action.toUpperCase().equals("SUBSCRIBE")) {
            var rawSubscription = recvMsg.pop();
            var subscription = Subscription.getRootAsSubscription(ByteBuffer.wrap(rawSubscription.getData()));

            for(int i = 0; i < subscription.endPointsLength(); ++i) {
                var endPoint = subscription.endPoints(i);
                var addressStr = String.format("tcp://%s:%d", endPoint.ipAddress(), endPoint.port());
                var socket = mCtx.createSocket(SocketType.SUB);
                if(!socket.connect(addressStr)) {
                    LOG.error("Failed to connect to " + addressStr);
                    continue;
                }
                socket.subscribe(String.format("%d", mSelfId));
                socket.subscribe(Common.EXG_TOPIC_ALL);
                mPoller.register(socket, ZMQ.Poller.POLLIN);
                mSocketSubscriptions.add(socket);
            }
        } else {
            LOG.error(String.format("Unrecognized action: \"%s\"", action));
        }
    }

    @Override
    public void run() {
        init();

        LOG.debug("Registering pollers...");
        mPoller= mCtx.createPoller(1);
        mPoller.register(mSocketControl, ZMQ.Poller.POLLIN);

        if(InitializeOnly) return;

        while(!Thread.currentThread().isInterrupted()) {
            mPoller.poll();
            if(mPoller.pollin(0)) {
                // PullControl
                handlePullControl(ZMsg.recvMsg(mSocketControl));
            }
            // rest of the subscriptions
            for(int i = 0; i < mSocketSubscriptions.size() - 1; ++i) {
                if(mPoller.pollin(i + 1)) {
                    // For now, we simply forward the message to coordinator
                    // with first frame (i.e. the subscription topic) removed
                    var socket = mSocketSubscriptions.get(i);
                    var recvMsg = ZMsg.recvMsg(socket);
                    if(recvMsg.size() < 1) {
                        LOG.error("Receive empty message");
                        continue;
                    }
                    // remove subscription topic frame
                    recvMsg.pop();
                    var forwardMsg = recvMsg.duplicate();
                    recvMsg.destroy();
                    forwardMsg.send(mSocketInternalLink);
                }
            }
        }
        mPoller.close();
    }
}
