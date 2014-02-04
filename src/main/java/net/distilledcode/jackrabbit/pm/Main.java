package net.distilledcode.jackrabbit.pm;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import net.distilledcode.jackrabbit.pm.commands.AbstractCommand;
import net.distilledcode.jackrabbit.pm.commands.ConsistencyCheck;
import net.distilledcode.jackrabbit.pm.commands.JackrabbitConsistencyCheck;
import net.distilledcode.jackrabbit.pm.commands.Remove;
import net.distilledcode.jackrabbit.pm.commands.TarOptimization;
import ch.qos.logback.classic.Logger;
import net.distilledcode.jackrabbit.pm.util.PMExecutionContext;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * TODO:
 * log to file (and console)
 * - accept --instructions-file option
 * - accept --repository-xml option
 * - accept --repository-url option (mirroring {@link org.apache.jackrabbit.commons.JcrUtils#getRepository(String uri)})
 * instruction file format:
 * rm path /content/dam
 * rm xpath /jcr:root/content/dam//*
 * rm sql-2 SELECT * ...
 * datastore-gc
 * tar-optimize
 * <p/>
 * how to express the constraint that an asset may not be referenced?
 * rm xpath /jcr:root/content/dam/geometrixx//*[@jcr:primaryType='dam:Asset' and @jcr:content/jcr:lastModified > xs:dateTime('2012-06-...')] with empty xpath /jcr:root/content/geometrixx/element(*, cq:Page)[jcr:contains(jcr:content, '$path')]
 */
public class Main {
    /**
     * The logger.
     */
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(Main.class);

    private static final Logger ROOT_LOGGER = (Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);

    public static void main(final String[] args) {
        final OptionParser parser = new OptionParser();
        parser.accepts("check", "Run custom consistency check.");
        parser.accepts("jr-check", "Run Jackrabbit PM consistency check.");
        parser.accepts("optimize", "Run TarPM optimization (only available on TarPM).");
        final OptionSpec<File> repoHome = parser
                .accepts("repository", "Path to the repository home directory.")
                .withRequiredArg()
                .ofType(File.class);
        final OptionSpec<String> wsName = parser
                .accepts("workspace")
                .withOptionalArg()
                .describedAs("Name of the persistence manager's workspace.")
                .defaultsTo("crx.default");
        final OptionSpec<String> remove = parser
                .accepts("remove")
                .withRequiredArg()
                .withValuesSeparatedBy(',')
                .describedAs("Comma separated list of paths to recursively remove.");
        final OptionSpec<String> log = parser
                .accepts("log")
                .withRequiredArg()
                .describedAs("Log level: debug, info, warn or error")
                .defaultsTo("info");


        final OptionSet optionSet = parser.parse(args);

        final File repositoryHome = repoHome.value(optionSet);
        final String workspaceName = wsName.value(optionSet);

        initializeLogFile();
        final String logLevel = optionSet.valueOf(log);
        ROOT_LOGGER.setLevel(Level.toLevel(logLevel, Level.INFO));

        LOG.info("--------------------------------------------------------------------------------");

        try {
            final AbstractCommand command;
            if (optionSet.has("check")) {
                command = new ConsistencyCheck();
            } else if (optionSet.has("jr-check")) {
                command = new JackrabbitConsistencyCheck();
            } else if (optionSet.has("optimize")) {
                command = new TarOptimization();
            } else if (optionSet.hasArgument("remove")) {
                final List<String> paths = remove.values(optionSet);
                command = new Remove(paths);
            } else {
                parser.printHelpOn(System.out);
                return;
            }


            final PMExecutionContext executionContext =
                    PMExecutionContext.create(repositoryHome.getAbsolutePath(), workspaceName);
            try {
                LOG.info("Running command {} now.", command.getClass().getSimpleName());
                command.execute(executionContext);
            } catch (Exception e) {
                LOG.error("Unexpected exception: ", e);
            } finally {
                TimeUnit.SECONDS.sleep(5);
                executionContext.dispose();
            }
        } catch (Exception e) {
            LOG.error("Something went horribly wrong: ", e);
        }
    }

    private static void initializeLogFile() {
        final LoggerContext loggerContext = ROOT_LOGGER.getLoggerContext();

        final PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setPattern("%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n");
        encoder.setContext(loggerContext);
        encoder.start();

        final FileAppender<ILoggingEvent> fileAppender = new FileAppender<ILoggingEvent>();
        fileAppender.setFile("migration.log");
        fileAppender.setName("file");
        fileAppender.setEncoder(encoder);
        fileAppender.setContext(loggerContext);
        fileAppender.start();

        ROOT_LOGGER.addAppender(fileAppender);
    }
}
