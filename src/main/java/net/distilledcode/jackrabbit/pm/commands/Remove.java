package net.distilledcode.jackrabbit.pm.commands;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import net.distilledcode.jackrabbit.pm.util.PMExecutionContext;
import org.apache.jackrabbit.core.persistence.PersistenceManager;
import org.apache.jackrabbit.core.state.ChangeLog;
import org.apache.jackrabbit.core.state.ItemStateException;
import org.apache.jackrabbit.core.state.NodeState;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Command to recursively remove content from a repository.
 */
public class Remove extends AbstractCommand {

    private static final Logger LOG = LoggerFactory.getLogger(Remove.class);

    private static final int SAVE_THRESHOLD = 5000;

    private final List<String> paths;

    private volatile int deletedCount;

    public Remove(List<String> paths) {
        this.paths = paths;
        this.deletedCount = 0;
    }

    @Override
    protected void doExecute(final PMExecutionContext executionContext) throws Exception {
        final PersistenceManager persistenceManager = executionContext.getPersistenceManager();
        for (final String path : paths) {
            for (final NodeState startNodeState : expandPath(persistenceManager, path)) {
                LOG.info("Recursively deleting {}", getPath(persistenceManager, startNodeState));
                recursiveDelete(persistenceManager, startNodeState);
            }
        }
    }

    private Iterable<NodeState> expandPath(final PersistenceManager pm, final String path) throws ItemStateException {
        LOG.info("Expanding path spec {}", path);
        final String relPath;
        if (path.startsWith("/")) {
            relPath = path.substring(1);
        } else {
            relPath = path;
        }
        final NodeState rootNodeState = getRootNodeState(pm);
        final NodeState nodeState = getNodeState(pm, rootNodeState, relPath);
        if (nodeState != null) {
            LOG.info("Found single NodeState at {}", relPath);
            return Collections.singletonList(nodeState);
        } else {
            final String name = Text.getName(relPath);
            final Predicate<String> filter = convertToNameFilterPredicate(name);
            if (filter == null) {
                LOG.info("NodeState {} does not exist; skipping", path);
                return Collections.emptyList();
            } else {
                final String parentPath = Text.getRelativeParent(relPath, 1);
                final NodeState parentState = getNodeState(pm, rootNodeState, parentPath);
                final List<NodeState> childNodeStates = getChildNodeStates(pm, parentState);
                final Iterable<NodeState> filteredChildren = Iterables.filter(childNodeStates, namePredicate(pm, filter));
                LOG.info("Found {} NodeStates at /{}", childNodeStates.size(), parentPath);
                return Lists.newArrayList(filteredChildren);
            }
        }
    }

    private Predicate<String> convertToNameFilterPredicate(final String filterExpression) {
        final int pos = filterExpression.indexOf("*");
        if (pos == -1) {
            return null;
        } else {
            final String start = filterExpression.substring(0, pos);
            final String end = filterExpression.substring(pos + 1, filterExpression.length());
            final Predicate<String> startsWith = startsWithPredicate(start);
            final Predicate<String> endsWith = endsWithPredicate(end);
            return Predicates.and(startsWith, endsWith);
        }
    }

    private Predicate<String> startsWithPredicate(final String startsWith) {
        return new Predicate<String>() {
            @Override
            public boolean apply(String string) {
                return string.startsWith(startsWith);
            }
        };
    }

    private Predicate<String> endsWithPredicate(final String endsWith) {
        return new Predicate<String>() {
            @Override
            public boolean apply(String string) {
                return string.endsWith(endsWith);
            }
        };
    }

    private static Predicate<NodeState> namePredicate(final PersistenceManager pm, final Predicate<String> nodeNamePredicate) {
        return new Predicate<NodeState>() {
            @Override
            public boolean apply(NodeState state) {
                try {
                    final String path = getPath(pm, state);
                    final String name = Text.getName(path);
                    final boolean matches = nodeNamePredicate.apply(name);
                    if (!matches) {
                        LOG.info("NodeState {} does not match the name pattern", path);
                    }
                    return matches;
                } catch (ItemStateException e) {
                    return false;
                }
            }
        };
    }

    private void recursiveDelete(PersistenceManager pm, NodeState startNodeState) throws ItemStateException {
        final List<NodeState> nodeStatesToDelete = internalRecursiveDelete(pm, startNodeState);
        nodeStatesToDelete.add(startNodeState);
        final NodeState parentNodeState = loadNodeState(pm, startNodeState.getParentId());
        persist(pm, parentNodeState, nodeStatesToDelete);
    }

    /**
     * Recursive deletion method.
     *
     * @param pm PersistenceManager used for accessing the persistence.
     * @param nodeState NodeState under which to recursively delete.
     * @return list of NodeStates to delete that have not yet been persisted.
     * @throws ItemStateException
     */
    private List<NodeState> internalRecursiveDelete(PersistenceManager pm, NodeState nodeState)
            throws ItemStateException {
        final List<NodeState> nodeStatesToDelete = new ArrayList<NodeState>();
        for (final NodeState child : getChildNodeStates(pm, nodeState)) {
            final List<NodeState> toDelete = internalRecursiveDelete(pm, child);
            nodeStatesToDelete.addAll(toDelete);
            if (nodeStatesToDelete.size() > SAVE_THRESHOLD) {
                persist(pm, nodeState, nodeStatesToDelete);
                nodeStatesToDelete.clear();
            }
        }
        nodeStatesToDelete.add(nodeState);
        return nodeStatesToDelete;
    }

    private void persist(PersistenceManager pm, NodeState nodeState, List<NodeState> childNodeStatesToDelete)
            throws ItemStateException {
        final ChangeLog changeLog = new ChangeLog();
        for (final NodeState toDelete : childNodeStatesToDelete) {
            if (nodeState.removeChildNodeEntry(toDelete.getNodeId())) {
                changeLog.modified(nodeState);
            }
            changeLog.deleted(toDelete);
        }
        pm.store(changeLog);
        deletedCount += childNodeStatesToDelete.size();
        LOG.info("Persisted {} (total: {}) deleted nodes under {}", childNodeStatesToDelete.size(), deletedCount, getPath(pm, nodeState));
    }
}
