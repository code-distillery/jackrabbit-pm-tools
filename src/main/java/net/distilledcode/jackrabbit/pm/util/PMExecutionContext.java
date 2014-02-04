package net.distilledcode.jackrabbit.pm.util;

import com.day.crx.core.config.CRXConfigurationParser;
import com.day.crx.persistence.CRXPMContext;
import com.day.crx.persistence.tar.TarPersistenceManager;
import org.apache.jackrabbit.core.NamespaceRegistryImpl;
import org.apache.jackrabbit.core.RepositoryImpl;
import org.apache.jackrabbit.core.config.PersistenceManagerConfig;
import org.apache.jackrabbit.core.config.RepositoryConfigurationParser;
import org.apache.jackrabbit.core.config.WorkspaceConfig;
import org.apache.jackrabbit.core.fs.FileSystem;
import org.apache.jackrabbit.core.fs.local.LocalFileSystem;
import org.apache.jackrabbit.core.id.NodeId;
import org.apache.jackrabbit.core.persistence.IterablePersistenceManager;
import org.apache.jackrabbit.core.persistence.PMContext;
import org.apache.jackrabbit.core.persistence.PersistenceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

import javax.jcr.NamespaceRegistry;
import javax.jcr.RepositoryException;
import java.io.File;
import java.io.FileReader;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;

/**
 * Execution context for PM Commands. The Execution context is
 * responsible for initializing the PersistenceManager and any
 * other repository services needed for command execution.
 */
public class PMExecutionContext {
    /**
     * The logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(PMExecutionContext.class);

    private PersistenceManager persistenceManager;

    private PMExecutionContext(PersistenceManager persistenceManager) {
        this.persistenceManager = persistenceManager;
    }

    public PersistenceManager getPersistenceManager() {
        return persistenceManager;
    }

    public CachingPersistenceManager getCachingPersistenceManager() {
        checkState(getPersistenceManager() instanceof IterablePersistenceManager, "IterablePersistenceManager required");
        return new CachingPersistenceManager((IterablePersistenceManager) getPersistenceManager(), 1000);
    }

    public static PMExecutionContext create(final String repoHome, final String workspaceName) throws Exception {

        final String workspaceHome = repoHome + "/workspaces/" + workspaceName;

        final LocalFileSystem repoFS = new LocalFileSystem();
        repoFS.setPath(repoHome + "/repository");
        repoFS.init();

        final NamespaceRegistryImpl namespaceRegistry = new NamespaceRegistryImpl(repoFS);

        final Properties variables = new Properties();
        variables.setProperty(RepositoryConfigurationParser.WORKSPACE_HOME_VARIABLE, workspaceHome);
        variables.setProperty(RepositoryConfigurationParser.WORKSPACE_NAME_VARIABLE, "crx.default");
        final CRXConfigurationParser parser = new CRXConfigurationParser(variables);

        final InputSource workspaceXml = new InputSource(new FileReader(workspaceHome + "/workspace.xml"));
        final WorkspaceConfig workspaceConfig = parser.parseWorkspaceConfig(workspaceXml);
        LOG.info("workspace config {}", workspaceConfig);
        final PersistenceManagerConfig pmConfig = workspaceConfig.getPersistenceManagerConfig();
        final PersistenceManager persistenceManager = pmConfig.newInstance(PersistenceManager.class);

        final File homeDir = new File(workspaceHome);
        initPM(persistenceManager, homeDir, workspaceConfig.getFileSystem(), namespaceRegistry);

        return new PMExecutionContext(persistenceManager);
    }

    public void dispose() throws Exception {
        persistenceManager.close();
    }

    private static void initPM(PersistenceManager pm, File home, FileSystem fs, NamespaceRegistry nr) throws Exception {
        pm.init(getPMContext(pm, home, fs, nr, RepositoryImpl.ROOT_NODE_ID));
    }

    private static PMContext getPMContext(PersistenceManager pm, File home, FileSystem fs, NamespaceRegistry nr, NodeId rootId)
            throws RepositoryException {
        if (pm instanceof TarPersistenceManager) {
            return new CRXPMContext(home, fs, rootId, nr, null, null, null, true, false, false);
        } else {
            return new PMContext(home, fs, rootId, nr, null, null);
        }
    }
}
