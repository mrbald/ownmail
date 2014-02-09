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
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import javax.mail.BodyPart;
import javax.mail.Header;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.internet.MimeMessage;

import net.htmlparser.jericho.Source;
import net.htmlparser.jericho.TextExtractor;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parallel mail indexer, with minor tweaks can be used to index anything.
 * <p>
 * Usage:<pre>
 *   java -cp ... net.bobah.mail.Indexer <config file>
 * </pre></p><p>
 * Sample configuration:<pre>
 *   # file system folder containing email messages
 *   net.bobah.mail.local.folder = .../gmail/gmailuser
 * 
 *   # number of parallel indexing threads, if not present then
 *   # the number of CPUs times two parallel threads are used
 *   net.bobah.mail.indexer.threads = 1
 * </pre></p>
 */
public class Indexer implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(Indexer.class);

    private static final String EMPTY_STRING = "".intern();

    private static String extractText(String contentType, Object content) {
        if (contentType.startsWith("text/")) {
            if (contentType.startsWith("text/plain")) {
                return (String)content;
            } else if (contentType.startsWith("text/html")) {
                return new TextExtractor(new Source((String)content)).toString();
            }
        } else {
            log.debug("ignored content type {}", contentType);
        }

        return EMPTY_STRING;
    }

    private void index(File file, IndexWriter writer, IndexSearcher searcher) throws IOException, MessagingException {
        if (searcher != null) {
            final Query query = new TermQuery(new Term("id", file.getName()));
            TopDocs docs = searcher.search(query, 1);
            if (docs.totalHits == 1) {
                int docID = docs.scoreDocs[0].doc;
                Document indexedDoc = searcher.doc(docID);
                final long indexedStamp = indexedDoc.getField("stamp").numericValue().longValue();
                if (indexedStamp >= file.lastModified()) {
                    log.debug("{} - unchanged", file.getName());
                    return;
                } else {
                    log.debug("{} - reindexing", file.getName());
                    writer.deleteDocuments(query);
                }
            } else {
                log.info("{} - indexing", file.getName());
            }
        } else {
            log.info("{} - indexing", file.getName());
        }

        final MimeMessage msg = readMessage(file, log);
        final Document doc = new Document();

        doc.add(new StringField("id", file.getName(), Store.YES));
        doc.add(new LongField("stamp", file.lastModified(), Store.YES));
        doc.add(new StringField("path", file.getAbsolutePath(), Store.YES));

        final Enumeration<?> headers = msg.getAllHeaders();
        while(headers.hasMoreElements()) {
            Header header = (Header)headers.nextElement();
            doc.add(new TextField(header.getName(), header.getValue(), Store.YES));
        }

        if (msg.getContentType() != null) {
            final String contentType = msg.getContentType().toLowerCase();
            if (contentType.startsWith("multipart/")) {
                final Multipart multiPart = (Multipart) msg.getContent();
                for (int i = 0; i < multiPart.getCount(); ++i) {
                    final BodyPart bodyPart = multiPart.getBodyPart(i);
                    doc.add(new TextField(String.format("body-%d", i), extractText(bodyPart.getContentType().toLowerCase(), bodyPart.getContent()), Store.NO));
                }
            } else {
                doc.add(new TextField("body", extractText(contentType, msg.getContent()), Store.NO));
            }
            writer.addDocument(doc);
        }
        //file.setLastModified(mime.getSentDate().getTime());
    }

    private final Properties config;
    private final ExecutorService executor;

    private Indexer(Properties config) {
        this.config = config;
        this.executor = createPooledExecutorService(Integer.valueOf(config.getProperty("net.bobah.mail.indexer.threads", String.valueOf(getOptimalExecutorServicePoolSize()))), "message-indexer");
    }

    @Override
    public void run() {
        try {
            runEx();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void runEx() throws Exception {
        final File dir = new File(config.getProperty("net.bobah.mail.local.folder"));
        if (!dir.exists() || !dir.isDirectory()) {
            throw new IllegalArgumentException(String.format("\"%s\" does not exist or is not a directory", dir));
        }

        Collection<File> files = findFiles(dir,
                new FileFilter() {
            @Override public boolean accept(File file) { return file.getName().endsWith(".eml"); }
        },
        new Comparator<File>() {
            @Override public int compare(File l, File r) { return Long.compare(l.lastModified(), r.lastModified()); }
        });

        Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_44);
        IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_44, analyzer);
        iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
        final File indexDir = new File(dir, "index");

        final boolean indexExisted = indexDir.exists();
        if (!indexExisted) indexDir.mkdirs();

        final Directory idx = FSDirectory.open(indexDir);
        final IndexWriter writer = new IndexWriter(idx, iwc);

        final IndexReader reader = indexExisted ? DirectoryReader.open(idx) : null;
        final IndexSearcher searcher = indexExisted ? new IndexSearcher(reader) : null;

        //final AtomicLong counter = new AtomicLong(0l);
        try {
            for (final File file: files) {
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            index(file, writer, searcher);
                            //if (counter.incrementAndGet() % 100 == 0) writer.commit(); // TODO: VL: make batch size configurable
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
            }

            shutdownExecutor(executor, log);

            // TODO: VL: delete stale documents from the index

            writer.commit();
            log.info("committed index updates");

            searcher.search(new MatchAllDocsQuery(), new Collector() {
                @Override
                public void setScorer(Scorer scorer) throws IOException {
                }

                @Override
                public void setNextReader(AtomicReaderContext unused) throws IOException {
                }

                @Override
                public void collect(int docID) throws IOException {
                    Document doc = reader.document(docID);
                    final String path = doc.get("path");
                    if (path != null) {
                        try {
                            final File file = new File(path);
                            if (!file.exists()) {
                                log.info("deleting index for {}", doc.get("id"));
                                writer.deleteDocuments(new Term("id", doc.get("id")));
                            }
                        } catch (SecurityException e) {
                            log.error("exception", e);
                        }
                    }
                }

                @Override
                public boolean acceptsDocsOutOfOrder() {
                    return true;
                }
            });

            writer.commit();
            log.info("committed index deletions");

        } finally {
            try {
                // close writer without commit (see explicit commits above)
                writer.rollback();
            } catch (IOException e) {
                log.error("exception while closing writer", e);
            }
        }
    }

    /**
     * @param args
     * @throws MessagingException 
     * @throws FileNotFoundException 
     */
    public static void main(String[] args) throws Exception {
        installDefaultUncaughtExceptionHandler(log);
        try {
            new Indexer(readConfig("config.properties")).run();
        } finally {
            shutdownLogger();
        }
    }

}
