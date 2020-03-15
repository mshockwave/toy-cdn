package uci.edu.cs230.toy_cdn;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import uci.edu.cs230.toy_cdn.fbs.RelayHeader;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

/**
 * Central dispatcher
 * */
public class Coordinator extends Thread {
    private final static Logger LOG = LogManager.getLogger(Coordinator.class);

    private ZContext mCtx;

    private ZMQ.Socket mSocketPullService, mSocketAE, mSocketPushService;

    /**
     * only used for testing
     * */
    boolean InitializeOnly = false;

    private static final class PollId {
        public static final int PULL_SERVICE = 0;
        public static final int ANALYSIS_ENGINE = 1;
        public static final int Size = 2;
    }

    public Coordinator(ZContext ipcContext) {
        mCtx = ipcContext;
    }

    private void init() {
        LOG.info("Initializing Coordinator Service...");

        // Special PAIR socket to synchronize between services
        ZMQ.Socket syncPullService, syncAE, syncPushService;

        syncPullService = mCtx.createSocket(SocketType.PAIR);
        syncPullService.bind("inproc://sync-coordinator-pull");
        // Wait for ready signal
        syncPullService.recv(0);
        syncPullService.close();
        LOG.debug("Subscribe to PullService");
        mSocketPullService = mCtx.createSocket(SocketType.PULL);
        mSocketPullService.connect("inproc://pullservice");

        syncAE = mCtx.createSocket(SocketType.PAIR);
        syncAE.bind("inproc://sync-coordinator-ae");
        // Wait for ready signal
        syncAE.recv(0);
        syncAE.close();
        LOG.debug("Subscribe to AnalysisEngine");
        mSocketAE = mCtx.createSocket(SocketType.PULL);
        mSocketAE.connect("inproc://ae");

        LOG.debug("Setup end point for PushService");
        mSocketPushService = mCtx.createSocket(SocketType.PUSH);
        mSocketPushService.bind("inproc://coordinator");
        syncPushService = mCtx.createSocket(SocketType.PAIR);
        syncPushService.connect("inproc://pushservice");
        syncPushService.send("READY", 0);
        syncPushService.close();
    }

    /**
     * Relay the message to PushService. Also:
     * 1. Remove any duplicate message
     * 2. Keep a copy of the file if needed
     * */
    static class RelayHandler implements MessageListener{
        private Set<Long> mRelayedFiles;

        public RelayHandler() {
            mRelayedFiles = new HashSet<>();
        }

        @Override
        public ZMsg onMessage(ZMsg message) {
            // expecting action header being removed from message
            assert message.size() > 1;
            var rawRelayHeader = message.peekFirst();
            var relayHeader = RelayHeader.getRootAsRelayHeader(ByteBuffer.wrap(rawRelayHeader.getData()));

            // Remove duplication
            if(mRelayedFiles.contains(relayHeader.requestSeq())) {
                message.destroy();
                // empty message
                return new ZMsg();
            }
            mRelayedFiles.add(relayHeader.requestSeq());

            // TODO: Keep a copy if needed

            var respMsg = message.duplicate();
            message.destroy();
            return respMsg;
        }
    }

    private void handlePullService() {

    }

    private void handleAnalysisEngine() {

    }

    @Override
    public void run() {
        init();
        if(InitializeOnly) return;

        LOG.debug("Registering pollers...");
        ZMQ.Poller poller = mCtx.createPoller(PollId.Size);
        poller.register(mSocketPullService, ZMQ.Poller.POLLIN);
        poller.register(mSocketAE, ZMQ.Poller.POLLIN);

        while(!Thread.currentThread().isInterrupted()) {
            poller.poll();
            if(poller.pollin(PollId.PULL_SERVICE)) {
                LOG.debug("Receive one event from PullService");
                handlePullService();
            }

            if(poller.pollin(PollId.ANALYSIS_ENGINE)) {
                LOG.debug("Receive one event from AnalysisEngine");
                handleAnalysisEngine();
            }
        }
    }
}
