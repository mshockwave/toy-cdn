package uci.edu.cs230.toy_cdn;

import com.google.flatbuffers.FlatBufferBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.*;
import uci.edu.cs230.toy_cdn.fbs.EndPoint;
import uci.edu.cs230.toy_cdn.fbs.FileExchangeHeader;
import uci.edu.cs230.toy_cdn.fbs.Subscription;
import uci.edu.cs230.toy_cdn.fbs.TraceNode;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Central dispatcher
 * */
public class Coordinator extends Thread {
    private final static Logger LOG = LogManager.getLogger(Coordinator.class);

    private ZContext mCtx;
    private long mSelfNodeId;
    private EndPointAddress mExternalAddress;

    /**
     * Single direction (i.e. Pull/Push) sockets
     * */
    private ZMQ.Socket mSocketPullService, mSocketAE, mSocketPushService;
    /**
     * Special purpose sockets:
     * HandShaking: A REP socket that accept external requests. Which are
     * usually request to subscribe to other clusters.
     * PullControl: An internal PAIR socket that send command to PullService.
     * Usually commands that tell PullService to subscribe to certain cluster.
     * */
    private ZMQ.Socket mSocketHandShaking, mSocketPullControl;

    private LocalStorageAgent mStorageAgent;
    private RespondHandler mRespondHandler;
    private RequestHandler mRequestHandler;
    private AnalysisEngineAgent mAEAgent;

    private List<EndPointAddress> mInitialNeighbors;

    /**
     * only used for testing
     * */
    boolean InitializeOnly = false;

    private static final class PollId {
        public static final int PULL_SERVICE = 0;
        public static final int ANALYSIS_ENGINE = 1;
        public static final int HAND_SHAKING = 2;
        public static final int Size = 3;
    }

    /**
     * For testing only
     * */
    Coordinator(ZContext ipcContext, long selfNodeId, EndPointAddress address, List<EndPointAddress> initialNeighbors) {
        mCtx = ipcContext;
        mSelfNodeId = selfNodeId;
        mExternalAddress = address;
        mInitialNeighbors = initialNeighbors;
    }

    public Coordinator(ZContext ipcContext, EndPointAddress address, List<EndPointAddress> initialNeighbors) {
        mCtx = ipcContext;
        mExternalAddress = address;
        mSelfNodeId = Common.getNodeId(mExternalAddress);
        mInitialNeighbors = initialNeighbors;
    }

    private void init() {
        LOG.info("Initializing Coordinator Service...");

        // Special PAIR socket to synchronize between services
        ZMQ.Socket syncPullService, syncAE, syncPushService;

        syncPullService = mCtx.createSocket(SocketType.PAIR);
        syncPullService.bind(Common.EP_INT_SYNC_COORDINATOR_PULL);
        // Wait for ready signal
        syncPullService.recv(0);
        syncPullService.close();
        mSocketPullService = mCtx.createSocket(SocketType.PULL);
        mSocketPullService.connect(Common.EP_INT_PULL_SERVICE);
        LOG.debug("Subscribe to PullService");
        mSocketPullControl = mCtx.createSocket(SocketType.PAIR);
        mSocketPullControl.connect(Common.EP_INT_PULL_CONTROL);
        LOG.debug("PullControl connected");

        syncAE = mCtx.createSocket(SocketType.PAIR);
        syncAE.bind(Common.EP_INT_SYNC_COORDINATOR_AE);
        // Wait for ready signal
        syncAE.recv(0);
        syncAE.close();
        LOG.debug("Subscribe to AnalysisEngine");
        mSocketAE = mCtx.createSocket(SocketType.PULL);
        mSocketAE.connect(Common.EP_INT_ANALYSIS_ENGINE);

        LOG.debug("Setup end point for PushService");
        mSocketPushService = mCtx.createSocket(SocketType.PUSH);
        mSocketPushService.bind(Common.EP_INT_COORDINATOR);
        syncPushService = mCtx.createSocket(SocketType.PAIR);
        syncPushService.connect(Common.EP_INT_SYNC_COORDINATOR_PUSH);
        syncPushService.send("READY", 0);

        // wait for ping back
        syncPushService.recv(0);
        syncPushService.close();

        LOG.debug("Setup handshaking end point");
        mSocketHandShaking = mCtx.createSocket(SocketType.REP);
        mSocketHandShaking.bind(String.format("tcp://%s:%s", mExternalAddress.IpAddress, mExternalAddress.Port));

        // Initialize components
        mStorageAgent = new LocalStorageAgent();
        mAEAgent = new AnalysisEngineAgent();
        mRespondHandler = new RespondHandler(mSelfNodeId, mAEAgent, mStorageAgent);
        mRequestHandler = new RequestHandler(mSelfNodeId, mStorageAgent);
    }

    private ZMsg subscribeToNeighbors(List<EndPointAddress> neighborEndPoints) {
        var builder = new FlatBufferBuilder(0);
        Subscription.startEndPointsVector(builder, neighborEndPoints.size());
        // reverse order
        for(int i = neighborEndPoints.size() - 1; i >= 0; --i) {
            var endPoint = neighborEndPoints.get(i);
            var ipAddressOffset = builder.createString(endPoint.IpAddress);
            // Their PushService would listen on port of HandShaking + 1
            EndPoint.createEndPoint(builder, ipAddressOffset, endPoint.Port + 1);
        }
        int endPointsOffset = builder.endVector();
        int subscription = Subscription.createSubscription(builder, endPointsOffset);
        builder.finish(subscription);

        var subscriptionMsg = new ZMsg();
        subscriptionMsg.add(Common.INT_ACTION_SUBSCRIBE);
        subscriptionMsg.add(builder.sizedByteArray());
        return subscriptionMsg;
    }

    private void initializeNeighbors() {
        // Phase 1. Subscribe to all neighbors
        var subscriptionMsg = subscribeToNeighbors(mInitialNeighbors);
        subscriptionMsg.send(mSocketPullControl);
        LOG.debug("Subscribe to neighbors");

        // Phase 2. Tell them to subscribe to me
        var builder = new FlatBufferBuilder(0);
        var addressOffset = builder.createString(mExternalAddress.IpAddress);
        int endPointOffset = EndPoint.createEndPoint(builder, addressOffset, mExternalAddress.Port);
        builder.finish(endPointOffset);

        var greetingMsg = new ZMsg();
        greetingMsg.add(Common.EXG_ACTION_NEW_NEIGHBOR);
        greetingMsg.add(builder.sizedByteArray());
        for(var neighbor : mInitialNeighbors) {
            var socket = mCtx.createSocket(SocketType.REQ);
            var addressStr = String.format("tcp://%s:%d", neighbor.IpAddress, neighbor.Port);
            if(!socket.connect(addressStr)) {
                LOG.error("Can not connect to neighbor " + addressStr);
                continue;
            }
            greetingMsg.duplicate().send(socket);
            socket.close();
        }
        LOG.debug("Greeting to neighbors");
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
         * Return file content bytes or Optional.empty if not exist
         * */
        Optional<byte[]> fetchFile(String fileId);

        /**
         * Put the file into local storage
         * */
        void putFile(String fileId, byte[] content);
    }

    static class LocalStorageAgent implements LocalStorageInterface {

        @Override
        public Optional<byte[]> fetchFile(String fileId) {
            return Optional.empty();
        }

        @Override
        public void putFile(String fileId, byte[] content) {

        }
    }

    /**
     * Detect request/respond message duplication based on the last sequence map provided.
     * Return true if there is a duplication. Return false and add the sequence to the map if otherwise.
     * */
    private static boolean handleExchangeMessageDuplication(FileExchangeHeader exchangeHeader, Map<Long, Integer> lastExchangeSeq) {
        var traceLen = exchangeHeader.traceLength();
        assert traceLen > 0;
        // Remove duplicate
        var finalStop = exchangeHeader.trace(0);
        if(lastExchangeSeq.containsKey(finalStop.nodeId())) {
            // Duplicate, do nothing
            return finalStop.sequence() <= lastExchangeSeq.get(finalStop.nodeId());
        }
        lastExchangeSeq.put(finalStop.nodeId(), finalStop.sequence());
        return false;
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

        private AnalysisQueryInterface mAnalysis;
        private LocalStorageInterface mLocalStorage;

        public RespondHandler(long selfNodeId,
                              AnalysisQueryInterface analysis,
                              LocalStorageInterface localStorage) {
            mLastRespondSeq = new HashMap<>();
            mSelfNodeId = selfNodeId;
            mAnalysis = analysis;
            mLocalStorage = localStorage;
        }

        private void saveFile(String fileId, ZMsg fileParts) {
            var byteStream = new ByteArrayOutputStream();
            int offset = 0;
            for (var frame : fileParts) {
                var size = frame.size();
                byteStream.write(frame.getData(), offset, size);
                offset += size;
            }
            mLocalStorage.putFile(fileId, byteStream.toByteArray());
        }

        @Override
        public ZMsg onMessage(ZMsg message) {
            assert message.size() > 1;
            var rawExchangeHeader = message.pop();
            var exchangeHeader = FileExchangeHeader.getRootAsFileExchangeHeader(ByteBuffer.wrap(rawExchangeHeader.getData()));
            if(handleExchangeMessageDuplication(exchangeHeader, mLastRespondSeq)) {
                return new ZMsg();
            }
            var traceLen = exchangeHeader.traceLength();

            var currentStop = exchangeHeader.trace(traceLen - 1);
            assert currentStop.nodeId() == mSelfNodeId;
            var fileId = exchangeHeader.fileId();
            if(traceLen == 1) {
                // I'm the recipient. Put the file into storage
                saveFile(fileId, message);
            } else {
                // Keep a copy if needed
                if(mAnalysis.keep(fileId)) {
                    saveFile(fileId, message);
                }

                // Remove current stop and relay to the neighbor next on the trace
                FlatBufferBuilder builder = new FlatBufferBuilder(0);
                var fileIdOffset = builder.createString(fileId);
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
                FileExchangeHeader.addFileId(builder, fileIdOffset);
                int newExchangeHeader = FileExchangeHeader.endFileExchangeHeader(builder);
                builder.finish(newExchangeHeader);

                // add rest of the original message body
                var relayMsg = message.duplicate();
                relayMsg.addFirst(builder.sizedByteArray());
                relayMsg.addFirst(Common.EXG_ACTION_RESPOND);
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
        /**
         * Work same as mLastRespondSeq in RespondHandler
         * */
        private Map<Long, Integer> mLastRequestSeq;

        private long mSelfNodeId;

        private LocalStorageInterface mLocalStorage;

        public RequestHandler(long selfNodeId, LocalStorageInterface localStorage) {
            mSelfNodeId = selfNodeId;
            mLocalStorage = localStorage;
            mLastRequestSeq = new HashMap<>();
        }

        @Override
        public ZMsg onMessage(ZMsg message) {
            // expecting action header being removed from message
            assert message.size() == 1;
            var rawExchangeHeader = message.pop();
            var exchangeHeader = FileExchangeHeader.getRootAsFileExchangeHeader(ByteBuffer.wrap(rawExchangeHeader.getData()));
            if(handleExchangeMessageDuplication(exchangeHeader, mLastRequestSeq)) {
                return new ZMsg();
            }

            var fileId = exchangeHeader.fileId();
            var file = mLocalStorage.fetchFile(fileId);
            if(file.isPresent()) {
                // Send the file
                var respondMsg = new ZMsg();
                respondMsg.add(Common.EXG_ACTION_RESPOND);
                respondMsg.add(rawExchangeHeader);
                respondMsg.add(file.get());
                return respondMsg;
            } else {
                // Relay the request
                // Append self to the trace
                FlatBufferBuilder builder = new FlatBufferBuilder(0);
                var fileIdOffset = builder.createString(fileId);
                var newTraceLen = exchangeHeader.traceLength() + 1;
                FileExchangeHeader.startTraceVector(builder, newTraceLen);
                // reverse order!
                // Sequence number doesn't matter for intermediate nodes
                TraceNode.createTraceNode(builder, mSelfNodeId, 0);
                for(int i = newTraceLen - 2; i >= 0; --i) {
                    var oldNode = exchangeHeader.trace(i);
                    TraceNode.createTraceNode(builder, oldNode.nodeId(), oldNode.sequence());
                }
                int newTrace = builder.endVector();

                int newExchangeHeader = FileExchangeHeader.createFileExchangeHeader(builder, newTrace, fileIdOffset);
                builder.finish(newExchangeHeader);

                var relayMsg = new ZMsg();
                relayMsg.add(Common.EXG_ACTION_REQUEST);
                relayMsg.add(builder.sizedByteArray());
                return relayMsg;
            }
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
            case Common.EXG_ACTION_RESPOND: {
                var relayMsg = mRespondHandler.onMessage(recvMsg);
                // Do nothing if it's empty message
                if(relayMsg.size() > 0) {
                    relayMsg.send(mSocketPushService);
                }
                break;
            }
            case Common.EXG_ACTION_REQUEST: {
                var respondMsg = mRequestHandler.onMessage(recvMsg);
                if(respondMsg.size() > 0) {
                    respondMsg.send(mSocketPushService);
                }
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
        // TODO
    }

    ZMsg handleHandShaking(ZMsg message) {
        assert message.size() >= 2;
        var action = message.popString();
        if(action.toUpperCase().equals(Common.EXG_ACTION_NEW_NEIGHBOR)) {
            // Subscribe to its PushService
            var rawEndPointHeader = message.pop().getData();
            var endPointHeader = EndPoint.getRootAsEndPoint(ByteBuffer.wrap(rawEndPointHeader));
            return subscribeToNeighbors(
                    List.of(new EndPointAddress(endPointHeader.ipAddress(), endPointHeader.port()))
            );
        } else {
            LOG.error(String.format("Unrecognized action \"%s\"", action));
            return new ZMsg();
        }
    }

    @Override
    public void run() {
        init();

        // Subscribe to initial sets of neighbors
        initializeNeighbors();

        if(InitializeOnly) return;

        LOG.debug("Registering pollers...");
        ZMQ.Poller poller = mCtx.createPoller(PollId.Size);
        poller.register(mSocketPullService, ZMQ.Poller.POLLIN);
        poller.register(mSocketAE, ZMQ.Poller.POLLIN);
        poller.register(mSocketHandShaking, ZMQ.Poller.POLLIN);

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

            if(poller.pollin(PollId.HAND_SHAKING)) {
                LOG.debug("Receive one event from HandShaking");
                var recvMsg = ZMsg.recvMsg(mSocketHandShaking);
                var outputMsg = handleHandShaking(recvMsg);
                recvMsg.destroy();
                if(outputMsg.size() > 0) {
                    // So far PullControl is the only place HandShaking
                    // will deliver message to
                    outputMsg.send(mSocketPullControl);
                }
            }
        }
    }
}
