package net.distilledcode.jackrabbit.pm.commands;

import com.google.common.collect.Sets;
import net.distilledcode.jackrabbit.pm.util.PMExecutionContext;
import org.apache.jackrabbit.core.id.NodeId;
import org.apache.jackrabbit.core.persistence.IterablePersistenceManager;
import org.apache.jackrabbit.core.persistence.PersistenceManager;
import org.apache.jackrabbit.core.state.ItemStateException;
import org.apache.jackrabbit.core.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Command to list all content paths. By default the paths are logged,
 * optionally they can be written to a specified file instead.
 */
public class PrintList extends AbstractCommand {

    private static final Logger LOG = LoggerFactory.getLogger(PrintList.class);

    private final PrintWriter output;
    private final String outputFileName;
    private final List<String> paths;

    public PrintList(final File output, final List<String> paths) throws IOException {
        if (output != null) {
            this.outputFileName = output.getName();
            this.output = new PrintWriter(output);
        } else {
            this.outputFileName = null;
            this.output = null;
        }
        this.paths = paths.isEmpty() ? Collections.singletonList("/") : paths;
    }

    @Override
    protected void doExecute(PMExecutionContext executionContext) throws Exception {
        if (output != null) {
            LOG.info("Output is written to {}", outputFileName);
        }

        final IterablePersistenceManager pm = executionContext.getCachingPersistenceManager();
        final NodeState rootNodeState = getRootNodeState(pm);
        for (final String path : paths) {
            checkArgument(path.startsWith("/"), "Path must start with a forward slash (/).");
            final NodeState nodeState = getNodeState(pm, rootNodeState, path.substring(1));
            if (nodeState == null) {
                LOG.warn("No node found for path {}", path);
            } else {
                listChildren(pm, nodeState);
            }
        }
        if (output != null) {
            output.flush();
            output.close();
        }
    }

    private void listChildren(final PersistenceManager pm, final NodeState parentState)
            throws ItemStateException {

        if (!parentState.isNode()) {
            return;
        }

        final String path = getPath(pm, parentState);
        if (output != null) {
            output.println(path);
        } else {
            LOG.info(path);
        }

        for (final NodeState childNodeState : getChildNodeStates(pm, parentState)) {
            listChildren(pm, childNodeState);
        }
    }
}
