package uci.edu.cs230.toy_cdn.registry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.util.Properties;

public class Main {
    private final static Logger LOG = LogManager.getLogger(Main.class);

    public static void main(String[] args) {
        var configStream = Main.class.getClassLoader().getResourceAsStream("registry.properties");
        assert configStream != null;
        var configProp = new Properties();
        try {
            configProp.load(configStream);
        } catch (IOException e) {
            LOG.error("Failed to read properties file");
            LOG.error(e);
            return;
        }

        ZContext ctx = new ZContext();
        ZMQ.Socket socket = ctx.createSocket(SocketType.REP);

        var address = configProp.getProperty("registry.address", "*");
        var port = configProp.getProperty("registry.port", "5555");
        socket.bind(String.format("tcp://%s:%s", address, port));
        LOG.info(String.format("Registry start listening on %s:%s ...", address, port));

        RegistrationListener regListener = new RegistrationListener();

        while(!Thread.currentThread().isInterrupted()) {
            byte[] rawReq = socket.recv(0);
            LOG.info("Receive message length " + rawReq.length);
            var rep = regListener.onMessage(rawReq);
            socket.send(rep, 0);
            LOG.info("Response with message length " + rep.length);
        }
    }
}
