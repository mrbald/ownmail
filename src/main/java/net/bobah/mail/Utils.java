/*
Copyright 2001-2014 Vladimir Lysyy
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this source code except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package net.bobah.mail;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;

import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;

/**
 * Reusable bits of code doing things the correct,
 * but not necessarily the fastest way.
 *
 */
final class Utils {
    private Utils() {}

    public static String formatBytes(long bytes) {
        return formatBytes(bytes, false);
    }

    /**
     * Human-friendly size formatter
     * 
     * @param bytes
     * @param si
     * @return
     */
    public static String formatBytes(long bytes, boolean si) {
        // http://stackoverflow.com/a/3758880/267482
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp-1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

    /**
     * Classpath config reader
     * 
     * @param filename
     * @return
     * @throws IOException
     */
    public static Properties readConfig(String filename) throws IOException {
        final Properties properties = new Properties();
        properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(filename));
        return properties;
    }

    /**
     * Installs uncaught exception handler that logs error and terminates the application.
     * 
     * @param log
     */
    public static void installDefaultUncaughtExceptionHandler(final Logger log) {
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                log.error("uncaught exception in thread " + t.getName(), e);
                System.exit(1);
            }
        });
    }

    /**
     * Creates a pooled {@link ExecutorService} of a fixed size and turns all threads
     * to daemons to make sure they do not block application shutdown.
     * 
     * @param poolSize
     * @param namePrefix
     * @return created {@link ExecutorService}
     */
    public static ExecutorService createPooledExecutorService(int poolSize, final String namePrefix) {
        return Executors.newFixedThreadPool(
                poolSize,
                new ThreadFactory() {
                    final AtomicInteger counter = new AtomicInteger(0);

                    @Override
                    public Thread newThread(Runnable runnable) {
                        final Thread thread = new Thread(runnable);
                        thread.setDaemon(true);
                        thread.setName(String.format("%s-%d", namePrefix, counter.incrementAndGet()));
                        return thread;
                    }
                });  
    }

    /**
     * Gracefully terminates the {@link ExecutorService} giving all tasks 365 days to complete.
     * 
     * @param executor
     * @param log
     * @throws InterruptedException
     */
    public static void shutdownExecutor(ExecutorService executor, Logger log) throws InterruptedException {
        log.info("waiting for all tasks to complete");
        executor.shutdown();
        if (executor.awaitTermination(365, TimeUnit.DAYS)) {
            log.info("all tasks completed");
        } else {
            log.error("timed out waiting for all tasks to complete");
        }
    }

    /**
     * BFS file searcher with file extension filter sorted by last modification date/time.
     * 
     * @param dir
     * @param ext
     * @return A collectons of files found
     * @see {@link #findFiles(File, FileFilter, Comparator)}
     */
    public static Collection<File> findFiles(final File dir, final String ext) {
        final String extLower = ext.toLowerCase();
        return findFiles(dir, new FileFilter() {
            @Override public boolean accept(File file) { return file.getName().toLowerCase().endsWith(extLower); }
        },
        new Comparator<File>() {
            @Override public int compare(File l, File r) { return Long.compare(l.lastModified(), r.lastModified()); }
        });
    }

    /**
     * BFS file searcher with custom filter and sorter.
     * 
     * @param dir
     * @param clientFilter
     * @param sorter
     * @return A collectons of files found
     */
    public static Collection<File> findFiles(final File dir, final FileFilter clientFilter, final Comparator<File> sorter) {
        final FileFilter filter = new FileFilter() {
            @Override
            public boolean accept(final File file) {
                return file.isDirectory() || (clientFilter == null || clientFilter.accept(file));
            }
        };

        // BFS recursive search
        final List<File> queue = new LinkedList<File>(Arrays.asList(dir.listFiles(filter)));

        for (ListIterator<File> itr = queue.listIterator(queue.size()); itr.hasPrevious();) {
            final File file = itr.previous();
            if (file.isDirectory()) {
                itr.remove();
                for (final File child: file.listFiles(filter)) {
                    itr.add(child);
                }
            }
        }

        if (sorter != null) Collections.sort(queue, sorter);
        return queue;
    }

    /**
     * Optimal pooled executor size
     * @return Returns number of available CPUs times 2.
     */
    public static int getOptimalExecutorServicePoolSize() {
        return Runtime.getRuntime().availableProcessors() * 2;
    }

    /**
     * Shortcut to load {@link MimeMessage} from file.
     * 
     * @param file
     * @param log
     * @return loaded {@link MimeMessage}
     * @throws IOException
     * @throws MessagingException
     */
    public static MimeMessage readMessage(File file, Logger log) throws IOException, MessagingException {
        InputStream is = null;
        try {
            is = new BufferedInputStream(new FileInputStream(file));
            return new MimeMessage(null, is);
        } finally {
            try { if (is != null) is.close(); } catch (IOException e) { if (log != null) log.error("exception", e); }
        }
    }

    /**
     * Gracefully shuts down the <a href="http://logback.qos.ch/">LOGBack</a> logger, required when using the asynchronous appender
     */
    public static void shutdownLogger() {
        ILoggerFactory loggerFactory = LoggerFactory.getILoggerFactory();
        if (loggerFactory instanceof LoggerContext) {
            LoggerContext context = (LoggerContext) loggerFactory;
            context.stop();
        }
    }
}
