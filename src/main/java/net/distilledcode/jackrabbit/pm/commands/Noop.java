package net.distilledcode.jackrabbit.pm.commands;

import net.distilledcode.jackrabbit.pm.util.PMExecutionContext;

/**
 * Command that does nothing except start up the PersistenceManager.
 */
public class Noop extends AbstractCommand {

    @Override
    protected void doExecute(final PMExecutionContext executionContext) throws Exception {

    }
}
