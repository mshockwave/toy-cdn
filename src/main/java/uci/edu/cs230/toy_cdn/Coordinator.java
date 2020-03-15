package uci.edu.cs230.toy_cdn;

import com.google.flatbuffers.FlatBufferBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import uci.edu.cs230.toy_cdn.fbs.FileExchangeHeader;
import uci.edu.cs230.toy_cdn.fbs.TraceNode;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Central dispatcher
 * */
public class Coordinator extends Thread {
    private final static Logger LOG = LogManager.getLogger(Coordinator.class);

    private ZContext mCtx;
    private long mSelfNodeId;

    private ZMQ.Socket mSocketPullService, mSocketAE, mSocketPushService;

    private LocalStorageAgent mStorageAgent;
    private RespondHandler mRespondHandler;
    private RequestHandler mRequestHandler;
    private AnalysisEngineAgent mAEAgent;

    /**
     * only used for testing
     * */
    boolean InitializeOnly = false;

    private static final class PollId {
        public static final int PULL_SERVICE = 0;
        public static final int ANALYSIS_ENGINE = 1;
        public static final int Size = 2;
    }

    public Coordinator(ZContext ipcContext, long selfNodeId) {
        mCtx = ipcContext;
        mSelfNodeId = selfNodeId;
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

        // Initialize components
        mStorageAgent = new LocalStorageAgent();
        mRespondHandler = new RespondHandler(mSelfNodeId);
        mAEAgent = new AnalysisEngineAgent();
    }

    interface AnalysisQueryInterface {
        /**
         * Return true if the given fileId should be put into
         * local storage
         * */
        boolean keep(String fileId);
    }

    interface LocalStorageInterface {
        /**
         * Fetch file content by the given fileId.
         * Return file content bytes or null if not exist
         * */
        byte[] fetchFile(String fileId);

        /**
         * Put the file into local storage
         * */
        void putFile(String fileId, byte[] content);
    }

    static class LocalStorageAgent implements LocalStorageInterface {

        @Override
        public byte[] fetchFile(String fileId) {
            return new byte[0];
        }

        @Override
        public void putFile(String fileId, byte[] content) {

        }
    }

    /**
     * Handling respond messages. Also:
     * 1. Remove duplicate message
     * 2. Fetch file to local storage if needed
     * */
    static class RespondHandler implements MessageListener{
        /**
         * Origin nodeId -> last responded sequence number.
         * If in the next message the origin (i.e. the first node in the trace)
         * sequence is smaller or equal than the recorded sequence, discard the
         * message.
         * */
        private Map<Long, Integer> mLastRespondSeq;

        private long mSelfNodeId;

        public RespondHandler(long selfNodeId) {
            mLastRespondSeq = new HashMap<>();
            mSelfNodeId = selfNodeId;
        }

        @Override
        public ZMsg onMessage(ZMsg message) {
            assert message.size() > 1;
            var rawExchangeHeader = message.pop();
            var exchangeHeader = FileExchangeHeader.getRootAsFileExchangeHeader(ByteBuffer.wrap(rawExchangeHeader.getData()));

            var traceLen = exchangeHeader.traceLength();
            assert traceLen > 0;
            // Remove duplicate
            var finalStop = exchangeHeader.trace(0);
            if(mLastRespondSeq.containsKey(finalStop.nodeId())) {
                if(finalStop.sequence() <= mLastRespondSeq.get(finalStop.nodeId())) {
                    // Duplicate, do nothing
                    return new ZMsg();
                }
            }
            mLastRespondSeq.put(finalStop.nodeId(), finalStop.sequence());

            var currentStop = exchangeHeader.trace(traceLen - 1);
            assert currentStop.nodeId() == mSelfNodeId;
            if(traceLen == 1) {
                // I'm the recipient
                // TODO
            } else {
                // TODO: Save one copy if needed

                // Remove current stop and relay to the neighbor next on the trace
                FlatBufferBuilder builder = new FlatBufferBuilder(0);
                var fileId = builder.createString(exchangeHeader.fileId());
                var newTraceLen = exchangeHeader.traceLength() - 1;
                FileExchangeHeader.startTraceVector(builder, newTraceLen);
                // reverse order!
                for(int i = newTraceLen - 1; i >= 0; --i) {
                    var oldNode = exchangeHeader.trace(i);
                    TraceNode.createTraceNode(builder, oldNode.nodeId(), oldNode.sequence());
                }
                int newTrace = builder.endVector();

                FileExchangeHeader.startFileExchangeHeader(builder);
                FileExchangeHeader.addTrace(builder, newTrace);
                FileExchangeHeader.addFileId(builder, fileId);
                int newExchangeHeader = FileExchangeHeader.endFileExchangeHeader(builder);
                builder.finish(newExchangeHeader);

                // add rest of the original message body
                var relayMsg = message.duplicate();
                relayMsg.addFirst(builder.sizedByteArray());
                return relayMsg;
            }

            return new ZMsg();
        }
    }

    /**
     * Handling file request. Either reply with file if found it
     * in local storage. Or broadcast the request to other neighbors
     * */
    static class RequestHandler implements MessageListener {

        public RequestHandler() {

        }

        @Override
        public ZMsg onMessage(ZMsg message) {
            // expecting action header being removed from message
            return null;
        }
    }

    private void handlePullService() {
        var recvMsg = ZMsg.recvMsg(mSocketPullService);
        if(recvMsg.size() <= 0) {
            LOG.error("Empty receive message");
            return;
        }

        var actionHeader = recvMsg.pop();
        var actionStr = actionHeader.getString(ZMQ.CHARSET);
        switch (actionStr.toUpperCase()) {
            case "RESPOND": {
                var relayMsg = mRespondHandler.onMessage(recvMsg);
                // Do nothing if it's empty message
                if(relayMsg.size() > 0) {
                    relayMsg.send(mSocketPushService);
                }
                break;
            }
            case "REQUEST": {
                break;
            }
            default:
                LOG.error(String.format("Unrecognized action \"%s\"", actionStr));
        }
        recvMsg.destroy();
    }

    /**
     * An in-memory storage/agent for the AnalysisEngine.
     * Listen the message from AnalysisEngine and provides
     * necessary interface toward other components in the Coordinator
     * */
    static class AnalysisEngineAgent implements MessageListener, AnalysisQueryInterface{

        @Override
        public boolean keep(String fileId) {
            return false;
        }

        @Override
        public ZMsg onMessage(ZMsg message) {
            return null;
        }
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
