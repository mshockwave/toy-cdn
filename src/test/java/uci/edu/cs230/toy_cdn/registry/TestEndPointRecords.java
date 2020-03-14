package uci.edu.cs230.toy_cdn.registry;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestEndPointRecords {
    private List<EndPointRecords.EndPointRecord> mRecords;

    @Before
    public void initArtifacts() {
        mRecords = new ArrayList<>();
        // DBH
        mRecords.add(new EndPointRecords.EndPointRecord(33.6434162f, -117.8423849f, "192.168.0.2", 4444));
        // In-N-Out
        mRecords.add(new EndPointRecords.EndPointRecord(33.6482452f, -117.8431137f, "192.168.0.4", 4444));
        // LAX
        mRecords.add(new EndPointRecords.EndPointRecord(33.941678f, -118.4106664f, "192.168.0.6", 4545));
        // Boston Public Library
        mRecords.add(new EndPointRecords.EndPointRecord(42.3494025f, -71.0788386f, "192.168.0.8", 6666));
    }

    @Test
    public void testOrder1() {
        var repo = new EndPointRecords();

        var neighbors = repo.join(mRecords.get(0), 15.0f, 1);
        Assert.assertEquals("Empty neighbor", 0, neighbors.size());
        neighbors = repo.join(mRecords.get(1), 15.0f, 1);
        Assert.assertEquals("One neighbor", 1, neighbors.size());

        neighbors = repo.join(mRecords.get(2), 100.0f, 1);
        Assert.assertEquals("Two neighbors", 2, neighbors.size());
        neighbors = repo.join(mRecords.get(3), 15.0f, 1);
        Assert.assertEquals("One neighbor", 1, neighbors.size());
    }
}
