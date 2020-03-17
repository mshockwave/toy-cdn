package uci.edu.cs230.toy_cdn;

import java.util.Optional;

public class LocalStorageAgent implements Coordinator.LocalStorageInterface {
    @Override
    public Optional<byte[]> fetchFile(String fileId) {
        return Optional.empty();
    }

    @Override
    public void putFile(String fileId, byte[] content) {

    }
}
