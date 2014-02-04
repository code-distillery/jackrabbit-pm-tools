package net.distilledcode.jackrabbit.pm.commands;

import net.distilledcode.jackrabbit.pm.util.PMExecutionContext;

public class JackrabbitConsistencyCheck extends AbstractCommand {

    @Override
    protected void doExecute(PMExecutionContext executionContext) throws Exception {
        executionContext.getPersistenceManager().checkConsistency(null, true, true);
    }
}
