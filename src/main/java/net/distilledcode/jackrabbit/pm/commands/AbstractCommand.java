package net.distilledcode.jackrabbit.pm.commands;

import net.distilledcode.jackrabbit.pm.util.PMExecutionContext;
import org.apache.jackrabbit.core.RepositoryImpl;
import org.apache.jackrabbit.core.id.NodeId;
import org.apache.jackrabbit.core.persistence.PersistenceManager;
import org.apache.jackrabbit.core.state.ChangeLog;
import org.apache.jackrabbit.core.state.ChildNodeEntry;
import org.apache.jackrabbit.core.state.ItemStateException;
import org.apache.jackrabbit.core.state.NoSuchItemStateException;
import org.apache.jackrabbit.core.state.NodeState;
import org.apache.jackrabbit.spi.Name;
import org.apache.jackrabbit.spi.commons.name.NameFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Abstract class for implementing commands. All commands extend from this class.
 */
public abstract class AbstractCommand {
    /**
     * The logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(AbstractCommand.class);

    /**
     * Flag to ensure this command is executed at most once.
     */
    private volatile AtomicBoolean alreadyExecuted = new AtomicBoolean(false);

    public void execute(final PMExecutionContext executionContext) throws Exception {
        checkState(!alreadyExecuted.getAndSet(true), "Already executed");
        doExecute(executionContext);
    }

    protected abstract void doExecute(final PMExecutionContext executionContext) throws Exception;

    // ____ UTILITY METHODS ____
    protected static NodeState getRootNodeState(final PersistenceManager pm) throws ItemStateException {
        return loadNodeState(pm, RepositoryImpl.ROOT_NODE_ID);
    }

    protected static void persist(PersistenceManager pm, ChangeLog changeLog) throws ItemStateException {
        if (changeLog.hasUpdates()) {
            pm.store(changeLog);
            changeLog.reset();
        }
    }

    protected static String getPath(PersistenceManager pm, NodeState nodeState) throws ItemStateException {
        checkNotNull(pm);
        checkNotNull(nodeState);
        if (nodeState.getParentId() == null) {
            return "";
        } else {
            final NodeState parentState = pm.load(nodeState.getParentId());
            final ChildNodeEntry childNodeEntry = parentState.getChildNodeEntry(nodeState.getNodeId());
            final String name = childNodeEntry.getName().getLocalName();
            return getPath(pm, parentState) + "/" + name;
        }
    }

    protected static NodeState loadNodeState(PersistenceManager pm, NodeId nodeId) {
        if (nodeId == null) {
            return null;
        }

        try {
            return pm.load(nodeId);
        } catch (ItemStateException e) {
            LOG.warn("error loading node ID {}", nodeId);
        }
        return null;
    }

    protected static List<NodeState> getChildNodeStates(final PersistenceManager pm, final NodeState nodeState) {
        checkArgument(nodeState != null, "NodeState must not be null.");
        final List<ChildNodeEntry> children = nodeState.getChildNodeEntries();
        final List<NodeState> childNodeStates = new ArrayList<NodeState>(children.size());
        for (final ChildNodeEntry childNodeEntry : children) {
            final NodeState child = loadNodeState(pm, childNodeEntry.getId());
            if (child == null) continue;
            childNodeStates.add(child);
        }
        return childNodeStates;
    }

    protected static NodeState getNodeState(PersistenceManager pm, NodeState parentState, String relPath)
            throws ItemStateException {
        checkArgument(!relPath.startsWith("/"), "Relative path expected", relPath);
        final String[] pathElements = relPath.split("/");
        final List<String> elements = new ArrayList<String>(pathElements.length);
        Collections.addAll(elements, pathElements);
        return getNodeState(pm, parentState, elements);
    }

    private static NodeState getNodeState(PersistenceManager pm, NodeState parentState, List<String> pathSegments)
            throws ItemStateException {
        checkNotNull(parentState, "parentState must not be null");
        if (!pathSegments.isEmpty() && pathSegments.get(0).length() > 0) {
            final Name name = NameFactoryImpl.getInstance().create("", pathSegments.remove(0));
            if (parentState.hasChildNodeEntry(name)) {
                final ChildNodeEntry childNodeEntry = checkNotNull(parentState.getChildNodeEntry(name, 1),
                        "parentState %s (%s) has no child %s",
                        getPath(pm, parentState), parentState.getNodeId(), name.getLocalName());
                try {
                    final NodeState child = pm.load(childNodeEntry.getId());
                    return getNodeState(pm, child, pathSegments);
                } catch (NoSuchItemStateException e) {
                    LOG.warn("Missing child entry detected: ", e);
                }
            } else {
                return null;
            }
        }
        return parentState;
    }
}
