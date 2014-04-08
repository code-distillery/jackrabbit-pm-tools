package net.distilledcode.jackrabbit.pm.commands;

import com.google.common.collect.Sets;
import net.distilledcode.jackrabbit.pm.util.PMExecutionContext;
import org.apache.jackrabbit.core.id.NodeId;
import org.apache.jackrabbit.core.persistence.IterablePersistenceManager;
import org.apache.jackrabbit.core.persistence.PersistenceManager;
import org.apache.jackrabbit.core.state.ChangeLog;
import org.apache.jackrabbit.core.state.ChildNodeEntry;
import org.apache.jackrabbit.core.state.ItemStateException;
import org.apache.jackrabbit.core.state.NoSuchItemStateException;
import org.apache.jackrabbit.core.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Command to run a consistency check on the given PersistenceManager.
 */
public class ConsistencyCheck extends AbstractCommand {
    /**
     * The logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(ConsistencyCheck.class);

    private AtomicLong processedNodeCounter = new AtomicLong();

    private long startTime;

    @Override
    protected void doExecute(final PMExecutionContext executionContext) throws Exception {
        startTime = System.currentTimeMillis();
        final ChangeLog changeLog = new ChangeLog();
        final IterablePersistenceManager persistenceManager = executionContext.getCachingPersistenceManager();
        checkChildren(persistenceManager, getRootNodeState(persistenceManager), changeLog);
        persist(persistenceManager, changeLog); // TODO: support dry-run
        //checkPMConsistency(persistenceManager); // TODO: support dry-run, re-implement equivalent functionality
    }

    private void checkChildren(PersistenceManager pm, NodeState startState, ChangeLog changeLog) throws ItemStateException, RepositoryException {
        final Set<NodeId> allNodeIds = getAllNodeIds(pm);
        final int nodeCount = allNodeIds.size();
        LOG.info("There are {} node IDs in the index", nodeCount);
        checkChildren(pm, startState, changeLog, allNodeIds, nodeCount);
        // TODO: handle remaining (orphaned) node ids.
        // TODO: implement basic checks as in jackrabbit (during the same traversal)
    }

    private void checkChildren(PersistenceManager pm, NodeState parentState, ChangeLog changeLog, Set<NodeId> allNodeIds, int nodeCount) throws ItemStateException {
        if (!parentState.isNode()) {
            return;
        }
        final String path = getPath(pm, parentState);
        final long nodesProcessed = processedNodeCounter.getAndIncrement();
        if (nodesProcessed % 10000 == 0 && nodesProcessed != 0) {
            long timeTaken = System.currentTimeMillis() - startTime;
            final long perNode = timeTaken / (nodesProcessed / 1000);
            LOG.info("processed {} nodes {} ({}) ({}ms/1k nodes, {}%)", nodesProcessed, path, parentState.getNodeId(), perNode, 100 * nodesProcessed / nodeCount);
        }

        allNodeIds.remove(parentState.getNodeId());

        for (final NodeState childNodeState : getChildNodeStates(pm, parentState)) {
            final String childPath = getPath(pm, childNodeState);
            assertParent(pm, parentState, childNodeState, changeLog, childPath);
            checkChildren(pm, childNodeState, changeLog, allNodeIds, nodeCount);
        }
    }

    private void assertParent(PersistenceManager pm, NodeState parent, NodeState child, ChangeLog changeLog, String path) throws ItemStateException {
        if (!parent.getNodeId().equals(child.getParentId()) && !child.containsShare(parent.getNodeId())) {
            LOG.warn("mismatching parent ids: {} claims to have child {}",
                    getPath(pm, parent), path, getPath(pm, pm.load(child.getParentId())));
            try {
                final NodeState otherParent = pm.load(child.getParentId());
                if (!otherParent.hasChildNodeEntry(child.getNodeId())) {
                    LOG.warn("orphaned child's parent {} does not reference the child {}", otherParent.getId(), child.getId());
                } else {
                    LOG.info("repairing {} by removing child {}", parent.getId(), child.getId());
                    parent.removeChildNodeEntry(child.getNodeId());
                    changeLog.modified(parent);
                }
            } catch (ItemStateException e) {
                LOG.error("boom", e);
            }
        }
    }


    private void checkPMConsistency(PersistenceManager persistenceManager) {
        persistenceManager.checkConsistency(null, true, true);
    }

    private Set<NodeId> getAllNodeIds(PersistenceManager pm) throws RepositoryException, ItemStateException {
        if (pm instanceof IterablePersistenceManager) {
            final IterablePersistenceManager ipm = (IterablePersistenceManager) pm;
            return Sets.newHashSet(ipm.getAllNodeIds(null, 0));
        }
        return Sets.newHashSet();
    }
}
