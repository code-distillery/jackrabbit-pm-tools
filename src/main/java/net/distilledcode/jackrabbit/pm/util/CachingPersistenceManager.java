package net.distilledcode.jackrabbit.pm.util;

import org.apache.jackrabbit.core.id.NodeId;
import org.apache.jackrabbit.core.id.PropertyId;
import org.apache.jackrabbit.core.persistence.IterablePersistenceManager;
import org.apache.jackrabbit.core.persistence.PMContext;
import org.apache.jackrabbit.core.persistence.PersistenceManager;
import org.apache.jackrabbit.core.state.ChangeLog;
import org.apache.jackrabbit.core.state.ItemStateException;
import org.apache.jackrabbit.core.state.NoSuchItemStateException;
import org.apache.jackrabbit.core.state.NodeReferences;
import org.apache.jackrabbit.core.state.NodeState;
import org.apache.jackrabbit.core.state.PropertyState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import java.util.concurrent.atomic.AtomicInteger;

public class CachingPersistenceManager implements IterablePersistenceManager {

    private static final Logger LOG = LoggerFactory.getLogger(CachingPersistenceManager.class);

    private final IterablePersistenceManager persistenceManager;

    private final AtomicInteger accesses = new AtomicInteger(0);

    private final AtomicInteger misses = new AtomicInteger(0);

    private final LRUCache<NodeId, NodeState> cache;
    public CachingPersistenceManager(IterablePersistenceManager pm, int cacheSize) {
        persistenceManager = pm;
        cache = new LRUCache<NodeId, NodeState>(cacheSize);
    }

    @Override
    public void init(PMContext context) throws Exception {
        persistenceManager.init(context);
    }

    @Override
    public void close() throws Exception {
        persistenceManager.close();
    }

    @Override
    public NodeState createNew(NodeId id) {
        return persistenceManager.createNew(id);
    }

    @Override
    public PropertyState createNew(PropertyId id) {
        return persistenceManager.createNew(id);
    }

    @Override
    public NodeState load(NodeId id) throws NoSuchItemStateException, ItemStateException {
        final int a = accesses.incrementAndGet();
        if (!cache.containsKey(id)) {
            misses.incrementAndGet();
            cache.put(id, persistenceManager.load(id));
        }
        if (a % 10000000 == 0) {
            LOG.info(getHitMissRatio());
        }
        return cache.get(id);
    }

    @Override
    public PropertyState load(PropertyId id) throws NoSuchItemStateException, ItemStateException {
        return persistenceManager.load(id);
    }

    @Override
    public NodeReferences loadReferencesTo(NodeId id) throws NoSuchItemStateException, ItemStateException {
        return persistenceManager.loadReferencesTo(id);
    }

    @Override
    public boolean exists(NodeId id) throws ItemStateException {
        return cache.containsKey(id) || persistenceManager.exists(id);
    }

    @Override
    public boolean exists(PropertyId id) throws ItemStateException {
        return persistenceManager.exists(id);
    }

    @Override
    public boolean existsReferencesTo(NodeId targetId) throws ItemStateException {
        return persistenceManager.existsReferencesTo(targetId);
    }

    @Override
    public void store(ChangeLog changeLog) throws ItemStateException {
        persistenceManager.store(changeLog);
        cache.clear();
    }

    @Override
    public void checkConsistency(String[] uuids, boolean recursive, boolean fix) {
        persistenceManager.checkConsistency(uuids, recursive, fix);
    }

    @Override
    public Iterable<NodeId> getAllNodeIds(NodeId after, int maxCount) throws ItemStateException, RepositoryException {
        return persistenceManager.getAllNodeIds(after, maxCount);
    }

    private String getHitMissRatio() {
        final int a = accesses.get();
        final int m = misses.get();
        final int h = a - m;

        final double percentage = (double) h / a * 100;
        final double hmRatio = (double) h / m;

        return String.format("Total accesses %d, (h=%d/m=%d => %.2f), cached %.2f%%", a, h, m, hmRatio, percentage);
    }
}
