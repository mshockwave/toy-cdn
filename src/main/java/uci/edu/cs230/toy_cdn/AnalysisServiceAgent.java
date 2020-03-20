package uci.edu.cs230.toy_cdn;

import com.google.flatbuffers.FlatBufferBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMsg;
import uci.edu.cs230.toy_cdn.fbs.FileExchangeHeader;
import uci.edu.cs230.toy_cdn.fbs.TraceNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * An in-memory storage/agent for the AnalysisService.
 * Listen the message from AnalysisService and provides
 * necessary interface toward other components in the Coordinator
 * */
public class AnalysisServiceAgent extends Coordinator.AnalysisAgentInterface {
    private final static Logger LOG = LogManager.getLogger(AnalysisServiceAgent.class);

    /**
     * Multi-Tier Ranking System
     * Every time a new ranking (from 1~10) from AnalysisService comes in
     * we inspect every ranks, which contains a file path. Find (or create if absent) the corresponding
     * entry in mMultiTierRankingMap, and increase the counter based on its ranks.
     * Tier0 means rank 1 and 2, Tier1 ranges from rank 3 ~ 5, and Tier2 ranges from
     * 6 ~ 10.
     * After the above step, we will inspect all map entries and do two reactions:
     * 1. For entry that has really high Tier0 counter value, we going to actively
     * fetch for it by sending REQUEST message
     * 2. For entry whose counter value is too low, remove from the map
     * For entry that has high Tier1 and/or Tier2 value (but not Tier0), we going to
     * fetch it if somebody ask via the keep function.
     * */
    private static class MultiTierRankingEntry {
        // Rank 1 and 2
        int Tier0Counter;
        // Rank 3 ~ 5
        int Tier1Counter;
        // Rank 6 ~ 10
        int Tier2Counter;

        /**
         * Counter value when this entry was added
         * */
        int JoinCounter;

        public float getT0Ratio(int current) {
            return ((float)Tier0Counter) / ((float)(current - JoinCounter));
        }

        public float getT1Ratio(int current) {
            return ((float)Tier1Counter) / ((float)(current - JoinCounter));
        }

        public float getT2Ratio(int current) {
            return ((float)Tier2Counter) / ((float)(current - JoinCounter));
        }

        public static MultiTierRankingEntry blank(int joinCounter) {
            var entry = new MultiTierRankingEntry();
            entry.Tier0Counter = 0;
            entry.Tier1Counter = 0;
            entry.Tier2Counter = 0;
            entry.JoinCounter = joinCounter;
            return entry;
        }
    }
    private Map<String, MultiTierRankingEntry> mMultiTierRankingMap;
    private int mCurrentCounter;

    private int mRequestSequence;

    private long mSelfId;

    public static final float ACTIVE_FETCH_THRESHOLD = 0.9f;
    public static final float PASSIVE_FETCH_THRESHOLD = 0.8f;
    public static final float PURGE_THRESHOLD = 0.2f;

    private Coordinator.LocalStorageInterface mLocalStorage;

    public AnalysisServiceAgent(long selfId, Coordinator.LocalStorageInterface localStorage) {
        mLocalStorage = localStorage;
        mMultiTierRankingMap = new HashMap<>();
        mCurrentCounter = 0;
        mRequestSequence = 0;
        mSelfId = selfId;
    }

    private List<String> searchForHot() {
        var hotFiles = mMultiTierRankingMap.entrySet().stream()
                .filter(e -> {
                    var entry = e.getValue();
                    var ratio = entry.getT0Ratio(mCurrentCounter);
                    return ratio > ACTIVE_FETCH_THRESHOLD;
                })
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        // Remove from the map
        for(var file : hotFiles) {
            mMultiTierRankingMap.remove(file);
        }
        return hotFiles;
    }

    private void purgeMap() {
        var toPurged = mMultiTierRankingMap.entrySet().stream()
                .filter(e -> {
                    var entry = e.getValue();
                    // the best ratio
                    var avgRatio = entry.getT0Ratio(mCurrentCounter);
                    avgRatio = Math.max(entry.getT1Ratio(mCurrentCounter), avgRatio);
                    avgRatio = Math.max(entry.getT2Ratio(mCurrentCounter), avgRatio);
                    return avgRatio < PURGE_THRESHOLD;
                })
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        if(toPurged.size() > 0)
            LOG.debug(String.format("Total of %d entries got purged", toPurged.size()));

        // Remove from the map
        for(var file : toPurged) {
            mMultiTierRankingMap.remove(file);
        }
    }

    @Override
    public boolean keep(String fileId) {
        // File exist
        if(mLocalStorage.fetchFile(fileId).isPresent())
            return false;

        if(mMultiTierRankingMap.containsKey(fileId)) {
            var entry = mMultiTierRankingMap.get(fileId);
            return entry.getT0Ratio(mCurrentCounter) > ACTIVE_FETCH_THRESHOLD ||
                    entry.getT1Ratio(mCurrentCounter) > PASSIVE_FETCH_THRESHOLD;
        }
        return false;
    }

    @Override
    public ZMsg onMessage(ZMsg message) {
        if(message.size() <= 0) {
            return new ZMsg();
        }

        LOG.error(String.format("Got one message of size %d", message.size()));

        for(int i = 0, len = Math.min(message.size(), 10); i < len; ++i) {
            var fileId = message.popString();
            var rank = i + 1;
            var entry = mMultiTierRankingMap.getOrDefault(fileId, MultiTierRankingEntry.blank(mCurrentCounter));
            if(rank <= 2) {
                // Tier0
                entry.Tier0Counter++;
            } else if(rank <= 5) {
                // Tier1
                entry.Tier1Counter++;
            } else {
                entry.Tier2Counter++;
            }
            mMultiTierRankingMap.put(fileId, entry);
        }
        mCurrentCounter++;

        var requestList = searchForHot();
        purgeMap();

        if(requestList.size() == 0)
            return new ZMsg();

        LOG.debug(String.format("Found %d hot entries", requestList.size()));

        // Double check if the file is really missing
        requestList.removeIf(fileId -> mLocalStorage.fetchFile(fileId).isPresent());
        LOG.debug(String.format("Requesting %d files...", requestList.size()));

        // constructing requests
        var reqMsg = new ZMsg();
        reqMsg.add(Common.EXG_ACTION_REQUEST);
        var builder = new FlatBufferBuilder(0);
        for(var requestFileId : requestList) {
            builder.clear();
            FileExchangeHeader.startTraceVector(builder, 1);
            TraceNode.createTraceNode(builder, mSelfId, mRequestSequence++);
            int traceOffset = builder.endVector();
            int fileIdOffset = builder.createString(requestFileId);
            int exchangeHeader = FileExchangeHeader.createFileExchangeHeader(builder, traceOffset, fileIdOffset);
            builder.finish(exchangeHeader);
            reqMsg.add(builder.sizedByteArray());
        }

        return reqMsg;
    }
}
