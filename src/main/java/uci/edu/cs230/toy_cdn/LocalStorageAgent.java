package uci.edu.cs230.toy_cdn;

import com.google.common.io.ByteStreams;
import net.markenwerk.utils.lrucache.LruCache;
import net.markenwerk.utils.lrucache.LruCacheListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class LocalStorageAgent implements Coordinator.LocalStorageInterface, LruCacheListener<String, String> {
    private final static Logger LOG = LogManager.getLogger(LocalStorageAgent.class);

    /**
     * fileId -> path in the HDFS
     * */
    private Map<String, String> mLocalCache;

    private FileSystem mFS;
    private Path mStorageRoot;

    /**
     * Put the files that are already in the FS into the cache
     * */
    private void synchronizeWithFS() {
        try {
            var itFiles = mFS.listFiles(mStorageRoot, false);
            while(itFiles.hasNext()) {
                var fsPath = itFiles.next().getPath();
                mLocalCache.put(fsPath.getName(), fsPath.toUri().getPath());
            }
        } catch (IOException e) {
            LOG.error("Failed to synchronized with existing FS content");
            LOG.error(e);
        }
    }

    private void initHDFS() {
        var configStream = getClass().getClassLoader().getResourceAsStream("hdfs.properties");
        assert configStream != null;
        var configProp = new Properties();
        try {
            configProp.load(configStream);
        } catch (IOException e) {
            LOG.error("Failed to read properties file");
            LOG.error(e);
            return;
        }

        var fsUri = configProp.getProperty("hdfs.uri", "hdfs://localhost:9000");
        LOG.debug(String.format("Using HDFS at %s ...", fsUri));
        try {
            var hadoopConfig = new Configuration();
            hadoopConfig.set("fs.defaultFS", fsUri);
            mFS = FileSystem.get(hadoopConfig);
        } catch (IOException e) {
            LOG.error("Failed to retrieve HDFS");
            LOG.error(e);
            return;
        }

        try {
            var rootPath = new Path(configProp.getProperty("hdfs.root"));
            mStorageRoot = new Path(configProp.getProperty("hdfs.storageRoot"));
            if(!mFS.exists(mStorageRoot)) {
                if(!mFS.exists(rootPath)) {
                    mFS.mkdirs(rootPath);
                }
                mFS.mkdirs(mStorageRoot);
            } else {
                synchronizeWithFS();
            }
        }catch (IOException e) {
            LOG.error("Failed to create root dir in HDFS");
            LOG.error(e);
        }
    }

    public LocalStorageAgent(int maxNumFile) {
        // Currently we don't support calculate by size of bytes
        mLocalCache = new LruCache<>(maxNumFile, this);
        initHDFS();
    }

    @Override
    public Optional<byte[]> fetchFile(String fileId) {
        // Use get method to make sure the LRU access got recorded
        var realPath = mLocalCache.get(fileId);
        if(realPath != null) {
            try{
                var fsPath = new Path(realPath);
                var stream = mFS.open(fsPath).getWrappedStream();
                var bytes = ByteStreams.toByteArray(stream);
                stream.close();
                return Optional.of(bytes);
            }catch (IOException e) {
                LOG.error(String.format("Failed to read file %s", realPath));
            }
        }
        return Optional.empty();
    }

    @Override
    public void putFile(String fileId, byte[] content) {
        var fsPath = new Path(mStorageRoot, fileId);
        var path = fsPath.toUri().getPath();
        try {
            var outputStream = mFS.create(fsPath);
            outputStream.write(content);
            mLocalCache.put(fileId, path);
            outputStream.close();
        } catch (IOException e) {
            LOG.error(String.format("Failed to create and write file %s", path));
            LOG.error(e);
        }
    }

    @Override
    public void onEvicted(Map.Entry<String, String> entry) {
        var realPath = entry.getValue();
        try {
            var fsPath = new Path(realPath);
            if(!mFS.delete(fsPath, true)) {
                LOG.error(String.format("Failed to delete %s", realPath));
            }
        } catch (IOException e) {
            LOG.error(e);
        }
    }
}
