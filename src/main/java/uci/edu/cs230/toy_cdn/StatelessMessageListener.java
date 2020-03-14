package uci.edu.cs230.toy_cdn;

public interface StatelessMessageListener {
    byte[] onMessage(byte[] message);
}
