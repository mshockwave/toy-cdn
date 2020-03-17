package uci.edu.cs230.toy_cdn;

import org.zeromq.ZMsg;

/**
 * An in-memory storage/agent for the AnalysisService.
 * Listen the message from AnalysisService and provides
 * necessary interface toward other components in the Coordinator
 * */
public class AnalysisServiceAgent extends Coordinator.AnalysisAgentInterface {
    @Override
    public boolean keep(String fileId) {
        return false;
    }

    @Override
    public ZMsg onMessage(ZMsg message) {
        return null;
    }
}
