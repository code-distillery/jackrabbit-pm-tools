package net.distilledcode.jackrabbit.pm;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import net.distilledcode.jackrabbit.pm.commands.AbstractCommand;
import net.distilledcode.jackrabbit.pm.commands.ConsistencyCheck;
import net.distilledcode.jackrabbit.pm.commands.JackrabbitConsistencyCheck;
import net.distilledcode.jackrabbit.pm.commands.PrintList;
import net.distilledcode.jackrabbit.pm.commands.Noop;
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
 * - accept --instructions-file option
 * instruction file format:
 * rm path /content/dam
 * rm xpath /jcr:root/content/dam//*
 * rm sql-2 SELECT * ...
 * datastore-gc
 * <p/>
 * how to express the constraint that an asset may not be referenced?
 * rm xpath /jcr:root/content/dam/geometrixx//*[@jcr:primaryType='dam:Asset' and @jcr:content/jcr:lastModified > xs:dateTime('2012-06-...')] with empty xpath /jcr:root/content/geometrixx/element(*, cq:Page)[jcr:contains(jcr:content, '$path')]
 */
public class Main {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(Main.class);

    private static final Logger ROOT_LOGGER = (Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);

    public static void main(final String[] args) {
        final OptionParser parser = new OptionParser();
        parser.accepts("check", "Run custom consistency check.");
        parser.accepts("jr-check", "Run Jackrabbit PM consistency check.");
        parser.accepts("optimize", "Run TarPM optimization (only available on TarPM).");
        parser.accepts("noop", "Start and stop the repository. May be used to trigger PM " +
                "specific initialization behaviour.");
        final OptionSpec<String> remove = parser.accepts("remove",
                "Comma separated list of paths to recursively remove.")
                .withRequiredArg()
                .withValuesSeparatedBy(',')
                .describedAs("path[,path]");
        final OptionSpec<String> list = parser.accepts("list",
                "List all paths under a given list of parent paths (comma separated).")
                .withRequiredArg()
                .withValuesSeparatedBy(',')
                .describedAs("path[, path]");
        final OptionSpec<File> outputFile = parser.accepts("outputFile",
                    "The filename or path of the file, to which output should be written.")
                .withRequiredArg()
                .describedAs("path").ofType(File.class);
        final OptionSpec<File> repoHome = parser
                .accepts("repository", "Path to the repository home directory.")
                .withRequiredArg()
                .ofType(File.class);
        final OptionSpec<String> wsName = parser
                .accepts("workspace", "Name of the persistence manager's workspace.")
                .withOptionalArg()
                .describedAs("workspace name")
                .defaultsTo("crx.default");
        final OptionSpec<String> log = parser
                .accepts("log", "Log level: debug, info, warn or error")
                .withRequiredArg()
                .describedAs("debug|info|warn|error")
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
            } else if (optionSet.hasArgument("list")) {
                final List<String> paths = list.values(optionSet);
                final File file = outputFile.value(optionSet);
                command = new PrintList(file, paths);
            } else if (optionSet.hasArgument("remove")) {
                final List<String> paths = remove.values(optionSet);
                command = new Remove(paths);
            } else if (optionSet.has("noop")) {
                command = new Noop();
            } else {
                parser.printHelpOn(System.out);
                return;
            }

            final PMExecutionContext executionContext =
                    PMExecutionContext.create(repositoryHome.getAbsolutePath(), workspaceName);
            final String name = command.getClass().getSimpleName();
            final long startTime = System.currentTimeMillis();
            try {
                LOG.info("Running command {} now.", name);
                command.execute(executionContext);
            } catch (Exception e) {
                LOG.error("Unexpected exception: ", e);
            } finally {
                TimeUnit.SECONDS.sleep(5);
                executionContext.dispose();
                LOG.info("Finished running command {} in {}ms.", name, System.currentTimeMillis() - startTime);
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
