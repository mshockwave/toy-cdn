package uci.edu.cs230.toy_cdn;

import com.google.common.hash.Hashing;
import zmq.ZMQ;

public class Common {
    /**
     * EndPoint Addresses
     */
    // Internal
    public static final String EP_INT_SYNC_COORDINATOR_PULL = "inproc://sync-coordinator-pull";
    public static final String EP_INT_SYNC_COORDINATOR_AE = "tcp://localhost:7849";
    public static final String EP_INT_SYNC_COORDINATOR_PUSH = "inproc://sync-coordinator-push";
    public static final String EP_INT_COORDINATOR = "inproc://coordinator";
    public static final String EP_INT_PULL_SERVICE = "inproc://pull-service";
    public static final String EP_INT_PULL_CONTROL = "inproc://pull-control";
    public static final String EP_INT_ANALYSIS_SERVICE = "tcp://localhost:7850";

    /**
     * Exchange Protocol Related
     * */
    public static final String EXG_ACTION_REQUEST = "REQUEST";
    public static final String EXG_ACTION_RESPOND = "RESPOND";
    public static final String EXG_ACTION_NEW_NEIGHBOR = "NEW_NEIGHBOR";

    public static final String EXG_TOPIC_ALL = "BROADCAST";

    /**
     * Internal Protocol Related
     * */
    public static final String INT_ACTION_SUBSCRIBE = "SUBSCRIBE";

    public static long getNodeId(EndPointAddress address) {
        return getNodeId(address.IpAddress, address.Port);
    }

    @SuppressWarnings("UnstableApiUsage")
    public static long getNodeId(String ipAddress, int port) {
        var hashFunc = Hashing.crc32();
        return hashFunc.newHasher()
                .putString(ipAddress, ZMQ.CHARSET)
                .putInt(port)
                .hash().asLong();
    }
}
