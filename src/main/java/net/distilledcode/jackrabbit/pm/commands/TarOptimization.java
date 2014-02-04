package net.distilledcode.jackrabbit.pm.commands;

import com.day.crx.persistence.tar.Optimize;
import com.day.crx.persistence.tar.TarPersistenceManager;
import com.day.crx.persistence.tar.TarSetHandler;
import net.distilledcode.jackrabbit.pm.util.PMExecutionContext;
import org.apache.jackrabbit.core.persistence.PersistenceManager;

import static com.google.common.base.Preconditions.checkState;

/**
 * Command to run TarOptimization on a TarPersistenceManager.
 *
 * If the PersistenceManager is not a TarPersistenceManager, an IllegalStateException is thrown.
 */
public class TarOptimization extends AbstractCommand {

    @Override
    protected void doExecute(final PMExecutionContext executionContext) throws Exception {
        final PersistenceManager persistenceManager = executionContext.getPersistenceManager();
        checkState(persistenceManager instanceof TarPersistenceManager, "TarPersistenceManager required");

        final TarPersistenceManager tpm = (TarPersistenceManager) persistenceManager;
        final TarSetHandler tarSet = tpm.getTarSet();
        final Optimize optimizer = tarSet.createOptimizer();
        optimizer.optimizeAllFiles(false, 100);
    }
}
