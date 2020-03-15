package uci.edu.cs230.toy_cdn;

public interface RawMessageListener {
    byte[] onMessage(byte[] message);
}
