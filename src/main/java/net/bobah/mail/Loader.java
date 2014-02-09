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

import static java.lang.String.format;
import static net.bobah.mail.Utils.*;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.LongBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.StandardOpenOption;
import java.util.Enumeration;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import javax.mail.FetchProfile;
import javax.mail.Folder;
import javax.mail.Header;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Store;
import javax.mail.UIDFolder;
import javax.mail.URLName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.mail.imap.IMAPFolder;

/**
 * Parallel IMAP (GMail) loader
 * <p>
 * Usage:<pre>
 *   java -cp ... net.bobah.mail.Indexer <config file>
 * </pre></p><p>
 * Sample configuration file:
 *   net.bobah.mail.host=imap.gmail.com
 *   net.bobah.mail.user=gmailuser
 *   net.bobah.mail.pass=***
 *   net.bobah.mail.protocol=imaps
 *   # can be used as a workaround against antivirus mitm mail scanner
 *   net.bobah.mail.ssl.trustall=true
 *   net.bobah.mail.remote.folder=[Gmail]/All Mail
 *   net.bobah.mail.local.folder=.../gmail/gmailuser
 *   net.bobah.mail.loader.sync.deleted=true
 *
 * </pre></p>
 */
public class Loader implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(Loader.class);

    private interface DfsCb {
        void process(IMAPFolder folder) throws MessagingException;
    };

    private static void dfs(IMAPFolder folder, DfsCb pre, DfsCb post) throws MessagingException {
        if (pre != null) pre.process(folder);
        else if (post == null) return;

        if ((folder.getType() & Folder.HOLDS_FOLDERS) == Folder.HOLDS_FOLDERS) {
            for (Folder sub: folder.list()) {
                dfs((IMAPFolder)sub, pre, post);
            }
        }

        if (post != null) post.process(folder);
    }

    private final Properties config;
    private final ExecutorService executor;

    private Loader(Properties config) {
        this.config = config;
        this.executor = createPooledExecutorService(Integer.valueOf(config.getProperty("net.bobah.mail.writer.threads", String.valueOf(getOptimalExecutorServicePoolSize()))), "message-writer");     
    }

    @Override
    public void run() {
        try {
            final String protocol = config.getProperty("net.bobah.mail.protocol");
            final String host = config.getProperty("net.bobah.mail.host");
            final String user = config.getProperty("net.bobah.mail.user");
            final String pass = config.getProperty("net.bobah.mail.pass");
            final boolean syncDeleted = Boolean.valueOf(config.getProperty("net.bobah.mail.loader.sync.deleted", "false")).booleanValue();

            final Properties sessionConfig = new Properties();
            if (Boolean.TRUE.equals(Boolean.valueOf(config.getProperty("net.bobah.mail.ssl.trustall", "false")))) {
                sessionConfig.setProperty(format("mail.%s.ssl.trust", protocol), "*");
            }
            Session session = Session.getDefaultInstance(sessionConfig, null);
            Store store = session.getStore(new URLName(protocol, host, -1, null, user, pass));
            store.connect();

            IMAPFolder folder = (IMAPFolder)store.getDefaultFolder();

            final boolean dumpSubfolderInfo = Boolean.valueOf(config.getProperty("net.bobah.mail.dumpsubfolders", "false"));
            if (dumpSubfolderInfo) {
                dfs(folder, new DfsCb() {
                    @Override
                    public void process(IMAPFolder folder) throws MessagingException {
                        if ((folder.getType() & Folder.HOLDS_MESSAGES) == Folder.HOLDS_MESSAGES) {
                            log.info("subfolder: {} ({}), {} messages", folder.getName(), folder.getFullName(), folder.getMessageCount());
                        }
                    }
                }, null);
            }

            final IMAPFolder allMail = (IMAPFolder)folder.getFolder(config.getProperty("net.bobah.mail.remote.folder", "[Gmail]/All Mail"));

            final File dir = new File(
                    config.getProperty("net.bobah.mail.local.folder", new File(host, user).getAbsolutePath()),
                    allMail.getName());
            log.info("local directory: {}", dir);

            if (!dir.exists()) {
                dir.mkdirs();
            } else if (!dir.isDirectory()) {
                throw new IllegalStateException(String.format("\"%s\" exists and is not a directory", allMail.getName()));
            }

            final File lastUidFile = new File(dir, "lastuid.sint64");
            final boolean lastUidFileExisted = lastUidFile.exists();
            final FileChannel lastSaved = FileChannel.open(lastUidFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
            final MappedByteBuffer mappedBuf = lastSaved.map(MapMode.READ_WRITE, 0, Long.SIZE/Byte.SIZE);
            final LongBuffer mappedLongBuf = mappedBuf.asLongBuffer();

            final long lastSeenUid;
            synchronized (mappedLongBuf) {
                if (!lastUidFileExisted) {
                    mappedLongBuf.put(0l);
                    mappedLongBuf.rewind();
                }
                lastSeenUid = mappedLongBuf.get();
                mappedLongBuf.rewind();
            }
            log.info("last seen uid in {}: {}", allMail.getName(), lastSeenUid);

            try {
                allMail.open(Folder.READ_ONLY);

                final AtomicLong totalBytes = new AtomicLong(0l);

                log.debug("requesting {}-{}", lastSeenUid + 1, UIDFolder.LASTUID);
                final Message[] allMsgs = allMail.getMessagesByUID(lastSeenUid + 1, UIDFolder.LASTUID);

                final FetchProfile fetchProfile = new FetchProfile();
                fetchProfile.add(FetchProfile.Item.CONTENT_INFO);
                log.info("fetching content info for {} message(s)", allMsgs.length);
                allMail.fetch(allMsgs, fetchProfile);

                msgs: for (final Message msg: allMsgs) {
                    final long uid = allMail.getUID(msg);
                    final File emlFile = new File(dir, format("%d.eml", uid));

                    if (msg.isExpunged()) {
                        if (syncDeleted && emlFile.exists()) {
                            emlFile.delete();
                            log.info("{} deleted on server - deleted locally", emlFile.getName());
                        }
                        continue;
                    }

                    if (uid <= lastSeenUid && emlFile.exists()) {
                        continue msgs;
                    }

                    final File hdrFile = new File(dir, format("%d.hdr", msg.getMessageNumber()));

                    final ByteArrayOutputStream buf = new ByteArrayOutputStream(msg.getSize());
                    msg.writeTo(buf);

                    executor.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                final boolean dumpHeaders = false;
                                if (dumpHeaders) {
                                    dumpHeaders(msg, hdrFile);
                                }

                                OutputStream emlOs = null;
                                try {
                                    if (emlFile.exists()) {
                                        log.error("file {} exists, rewriting {}", emlFile);
                                        emlFile.delete();
                                    }

                                    emlOs = new FileOutputStream(emlFile);
                                    buf.writeTo(emlOs);
                                    emlOs.close();
                                    log.info("written {} to {}, ({} session total)", formatBytes(msg.getSize()), emlFile.getName(), formatBytes(totalBytes.addAndGet(msg.getSize())));
                                } finally {
                                    if (emlOs != null) {
                                        try {
                                            emlOs.close();
                                        } catch (Exception e) {
                                            log.error("exception while closing {}", emlFile.getName());
                                        }
                                    }
                                }

                                synchronized (mappedLongBuf) {
                                    long persistedUid = mappedLongBuf.get();
                                    mappedLongBuf.rewind();
                                    if (persistedUid < uid) {
                                        mappedLongBuf.put(uid);
                                        mappedLongBuf.rewind();
                                        log.debug("updated last seen uid in {}: {} -> {}", allMail.getName(), persistedUid, uid);
                                    } else {
                                        log.debug("retained last seen uid in {}: {}", allMail.getName(), persistedUid);
                                    }
                                }  
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                    });
                }

                shutdownExecutor(executor, log);
            } finally {
                try { if (allMail.isOpen()) allMail.close(false); } catch (MessagingException unused) {}
                try { if (lastSaved.isOpen()) lastSaved.close();} catch (IOException unused) {}
            }
        } catch (Exception e) {
            log.error("exception", e);
            System.exit(1);
        }
    }

    private static void dumpHeaders(final Message msg, final File hdrFile)
            throws FileNotFoundException, MessagingException {
        PrintStream hdrOs = null;
        try {
            hdrOs = new PrintStream(new BufferedOutputStream(new FileOutputStream(hdrFile)));
            Enumeration<?> headers = msg.getAllHeaders();
            while (headers.hasMoreElements()) {
                Header header = (Header)headers.nextElement();
                hdrOs.println(format("%s: %s", header.getName(), header.getValue()));
            }
            log.info("written headers to {}", hdrFile.getName());
        } finally {
            if (hdrOs != null) try { hdrOs.close(); } catch (Exception e) {}
        }
    }

    public static void main(String[] args) throws IOException {
        installDefaultUncaughtExceptionHandler(log);
        try {
            new Loader(readConfig("config.properties")).run();
        } finally {
            shutdownLogger();
        }
    }
}
