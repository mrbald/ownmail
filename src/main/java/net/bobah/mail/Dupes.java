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
import static net.bobah.mail.Utils.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.text.DateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.collect.Collections2;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Ints;

/** 
 * Fuzzy duplicate files searcher.
 * 
 * Reports files having the same hash function. Was written to detect
 * dupes in email-s, mp3-s and photos
 * 
 */
public class Dupes implements Runnable {
    final ExecutorService executor;
    final HashFunction hashfunc;
    final File[] dirs;

    private Dupes(final int threads, final HashFunction hashfunc, final File ... dirs) {
        this.executor = createPooledExecutorService(threads, Dupes.class.getSimpleName());;
        this.hashfunc = hashfunc;
        this.dirs = dirs;
    }

    private static final Logger log = LoggerFactory.getLogger(Dupes.class);

    private static final class ExecutionContext {
        public final Stopwatch sw = Stopwatch.createUnstarted();
        public final byte[] buf = new byte[1<<20];

        public void recycle() {
            if (sw.isRunning()) sw.stop();
            sw.reset();
        }
    };

    private final ThreadLocal<ExecutionContext> cxt = new ThreadLocal<ExecutionContext>() {
        @Override protected final ExecutionContext initialValue() {
            log.debug("created execution context");
            return new ExecutionContext();
        }
    };

    private enum Hashes {
        crc32(Hashing.crc32()),
        adler32(Hashing.adler32()),
        fast64(Hashing.goodFastHash(64)),
        md5(Hashing.md5());

        final HashFunction hashfunc;
        Hashes(final HashFunction hashfunc) {
            this.hashfunc = hashfunc;
        }
    }

    // TODO: VL: - compare checksum of the <1mb of a file
    //           - for detected dupes compare size, then for those remaining if > 2 in set compare hashes on all, else compare byte-to-byte.
    @Override
    public void run() {
        final Multimap<HashCode, File> dupes = Multimaps.newListMultimap(new HashMap<HashCode, Collection<File>>(), new Supplier<List<File>>() {
            @Override public List<File> get() {
                return new LinkedList<File>();
            }
        });

        for (final File dir: dirs) {
            if (!dir.isDirectory()) {
                log.warn("{} does not exist or is not a directory, ignored", dir);
            }

            final Collection<File> files = findFiles(dir, "");
            log.info("found {} files in {}, submitting to analyzer", files.size(), dir.getAbsolutePath());


            for (final File file: files) {
                executor.submit(new Runnable() {
                    @Override public void run() {
                        final ExecutionContext cxt = Dupes.this.cxt.get();

                        ReadableByteChannel ch = null;
                        try {
                            cxt.sw.start();
                            // map file, take just 1 meg of data to cxt.hash and calc the function
                            // final HashCode code = Files.hash(file, hashfunc);
                            ch = Channels.newChannel(new FileInputStream(file));
                            ByteBuffer buf = ByteBuffer.wrap(cxt.buf);
                            final int len = ch.read(buf);
                            if (len == 0) return;

                            final HashCode code = hashfunc.hashBytes(cxt.buf, 0, Ints.checkedCast(len));
                            synchronized (dupes) { dupes.put(code, file); }

                            cxt.sw.stop();
                            log.debug("{} -> {} ({}) - {} us", file, code, DateFormat.getInstance().format(file.lastModified()), cxt.sw.elapsed(TimeUnit.MILLISECONDS));
                        } catch (Exception e) {
                            log.debug("exception", e);
                        } finally {
                            cxt.recycle();
                            if (ch != null) try { ch.close(); } catch (IOException unused) {};
                        }
                    }
                });
            }
            log.info("done submitting {} to analyzer", dir.getAbsolutePath());
        }

        try {
            shutdownExecutor(executor, log);
        } catch (InterruptedException e) {
            log.debug("exception", e);
        }

        for(Collection<File> filez: dupes.asMap().values()) {
            if (filez.size() == 1) continue;
            log.info("dupes found: {}", filez);
        }
    }

    public static void main(String[] args) throws Exception {
        installDefaultUncaughtExceptionHandler(log);

        final CommandLineParser parser = new PosixParser();
        final Options options = new Options()
        .addOption("j", "threads", true, "number of parallel threads to use for analyzing")
        .addOption("hash", true, "hash function to use, possible values: " + Arrays.toString(Hashes.values()))
        .addOption("dir", true, "add directory to search");
        final CommandLine cmdline = parser.parse(options, args);

        final int threads = Integer.valueOf(cmdline.getOptionValue("threads", String.valueOf(Runtime.getRuntime().availableProcessors())));
        final HashFunction hash = Hashes.valueOf(cmdline.getOptionValue("hash", "adler32")).hashfunc;
        final File[] dirs = Collections2.transform(Arrays.asList(cmdline.getOptionValues("dir")), new Function<String, File>() {
            @Override public File apply(String from) {
                return new File(from);
            }
        }).toArray(new File[]{});

        log.info("hash: {}, threads: {}, dirs: {} in total", hash, threads, dirs.length);
        try {
            new Dupes(threads, hash, dirs).run();
        } finally {
            Utils.shutdownLogger();
        }
    }
}
