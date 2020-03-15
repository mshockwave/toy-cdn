package uci.edu.cs230.toy_cdn;

import org.zeromq.ZMsg;

public interface MessageListener {
    ZMsg onMessage(ZMsg message);
}
