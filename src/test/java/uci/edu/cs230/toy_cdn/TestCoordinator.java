package uci.edu.cs230.toy_cdn;

import org.junit.Assert;
import org.junit.Test;
import org.zeromq.ZContext;

public class TestCoordinator {
    @Test
    public void testInitialization() {
        ZContext ctx = new ZContext();

        var coordinator = new Coordinator(ctx);
        coordinator.InitializeOnly = true;
        var pullService = new PullService(ctx);
        pullService.InitializeOnly = true;
        var analysisEngine = new AnalysisEngineService(ctx);
        analysisEngine.InitializeOnly = true;

        try {
            analysisEngine.start();
            pullService.start();
            coordinator.start();

            pullService.join();
            analysisEngine.join();
            coordinator.join();
        }catch (InterruptedException e) {
            throw new AssertionError("Interrupted");
        }
    }
}
