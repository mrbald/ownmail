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

import static java.lang.System.exit;
import static net.bobah.mail.Utils.readMessage;
import static net.bobah.mail.Utils.shutdownLogger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteSource;
import com.google.common.io.CharStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.common.net.HttpHeaders;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import freemarker.cache.ClassTemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.DefaultObjectWrapper;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;

/**
 * A *very* simple web interface to search the index and download the files.
 * Using JDK built in HTTP server and FreeMarker as a template engine.
 * <p>
 * The search page URL is <a href="http://localhost:8080/search">http://localhost:8080/search</a>
 * </p><p>
 * Usage:<pre>
 *   java -cp ... net.bobah.mail.Searcher <index root dir>
 * </pre></p><p>
 * Example:<pre>
 *   java -cp .../mailocal.jar net.bobah.mail.Searcher .../gmail/gmailuser
 * </pre></p>
 */
public class Searcher {
    private static final Logger log = LoggerFactory.getLogger(Searcher.class);
    private static final String SEARCH_CONTEXT = "/search";
    private static final String FETCH_CONTEXT = "/fetch";

    private final Configuration cfg = new Configuration() {{
        setTemplateLoader(new ClassTemplateLoader(Searcher.class, "/fmtpl"));
        setDefaultEncoding("UTF-8");
        setObjectWrapper(new DefaultObjectWrapper());
        setTemplateExceptionHandler(TemplateExceptionHandler.HTML_DEBUG_HANDLER); // TemplateExceptionHandler.RETHROW_HANDLER
    }};

    private final Template tpl;
    private final IndexReader reader;
    private final IndexSearcher searcher;
    private final Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_44);
    private final QueryParser parser = new QueryParser(Version.LUCENE_44, "body", analyzer);

    public Searcher(File indexDir) throws Exception {
        tpl = cfg.getTemplate("index.fm");

        reader = DirectoryReader.open(FSDirectory.open(indexDir));
        searcher = new IndexSearcher(reader);
    }

    public static final class Data {
        private static final MimeMessage[] NOTHING = new MimeMessage[]{};
        private String query = "".intern();
        private String searchActionPath = SEARCH_CONTEXT;
        private String fetchActionPath = FETCH_CONTEXT;
        private MimeMessage[] messages = NOTHING;

        public Data() {}

        public String getQuery() {
            return query;
        }

        public void setQuery(String query) {
            this.query = query;
        }

        public MimeMessage[] getMessages() {
            return messages;
        }

        public void setMessages(MimeMessage[] messages) {
            this.messages = messages;
        }

        public String getSearchActionPath() {
            return searchActionPath;
        }

        public String getFetchActionPath() {
            return fetchActionPath;
        }
    };

    private static final Splitter.MapSplitter formDataSplitter = Splitter.on('&').trimResults().withKeyValueSeparator('=');
    private static final Splitter headerValueSplitter = Splitter.on(';').trimResults();

    private static final String contentType(final Headers reqHeaders) {
        final String reqContentType = reqHeaders.getFirst(HttpHeaders.CONTENT_TYPE);
        String encoding = "utf-8";
        if (!Strings.isNullOrEmpty(reqContentType)) {
            final Iterable<String> params = headerValueSplitter.split(reqContentType);
            final String param = Iterables.find(params, new Predicate<String>() {
                @Override public boolean apply(String param) {
                    return param.startsWith("encoding=");
                }
            }, "encoding=utf-8");
            encoding = param.substring("encoding=".length());
        }
        return encoding;
    }

    private final HttpHandler searchHandler = new HttpHandler() {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            final String method = exchange.getRequestMethod();
            log.info("{} {}", method, exchange.getRemoteAddress());

            if (!"POST".equals(method) && !"GET".equals(method)) {
                exchange.sendResponseHeaders(403, -1);
                return;
            }

            final Headers reqHeaders = exchange.getRequestHeaders();
            for (Map.Entry<String, List<String>> reqHeader: reqHeaders.entrySet()) {
                log.info("{}: {}", reqHeader.getKey(), reqHeader.getValue());
            }

            final String encoding = contentType(reqHeaders);

            final Data data = new Data();

            if ("POST".equals(method)) {
                InputStream input = exchange.getRequestBody();
                log.info("request, bytes: {}", input.available());
                final String formData = CharStreams.toString(new InputStreamReader(input));
                log.info("request:\n {}", formData);
                Map<String, String> formDataMap = formDataSplitter.split(formData);
                log.info("form:\n {}", formDataMap);
                for (String query = formDataMap.get("query"); !Strings.isNullOrEmpty(query); query = null) data.setQuery(URLDecoder.decode(query, encoding));
            }

            if (!Strings.isNullOrEmpty(data.getQuery())) {
                log.info("searching for [{}]", data.getQuery());
                // process search request
                try {
                    final TopDocs topDocs = searcher.search(parser.parse(data.getQuery()), 25);

                    MimeMessage[] messages = new MimeMessage[Math.min(topDocs.scoreDocs.length, 25)];
                    for (int i = 0; i < messages.length; ++i) {
                        final String path = reader.document(topDocs.scoreDocs[i].doc).get("path");
                        final MimeMessage msg = readMessage(new File(path), null);
                        msg.setFileName(path);
                        messages[i] = msg;
                    }

                    data.setMessages(messages);
                    log.info("search complete, found {} messages", messages.length);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else {
                log.info("no search request");
            }

            Headers respHeaders = exchange.getResponseHeaders();
            respHeaders.set(HttpHeaders.CONTENT_TYPE, "text/html;charset=utf-8");
            exchange.sendResponseHeaders(200, 0);
            OutputStream body = exchange.getResponseBody();
            OutputStreamWriter out = new OutputStreamWriter(body);
            try {
                log.info("instantiating page template");
                tpl.process(data, out);
                log.info("instantiated page template");
            } catch (TemplateException e) {
                throw new IOException(e);
            } finally {
                Closeables.close(out, true);
            }
        }
    };

    private final HttpHandler fetchHandler = new HttpHandler() {   
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            final String method = exchange.getRequestMethod();
            log.info("{} {}", method, exchange.getRemoteAddress());

            OutputStream body = exchange.getResponseBody();
            try {
                if ("GET".equals(method)) {
                    final String encoding = contentType(exchange.getRequestHeaders());

                    final String request = exchange.getRequestURI().getQuery();
                    log.info("request: {}", request);
                    Map<String, String> params = formDataSplitter.split(request);

                    for (String param = params.get("file"); !Strings.isNullOrEmpty(param); param = null) {
                        final File file = new File(URLDecoder.decode(param, encoding));
                        if (file.exists() && file.isFile()) {
                            log.info("copying file: {}, size: {}", file, file.length());

                            exchange.getResponseHeaders().set(HttpHeaders.CONTENT_TYPE, "message/rfc822");
                            try {
                                ByteSource from = Files.asByteSource(file);
                                exchange.sendResponseHeaders(200, file.length());
                                from.copyTo(body);
                                log.info("copied file: {}, size: {}", file, file.length());
                            } catch (Exception e) {
                                log.error("exception", e);
                            }
                        } else {
                            log.info("path {} does not exist or is not a file", file);
                            exchange.sendResponseHeaders(404, 0);
                            return;
                        }
                    }
                } else {
                    exchange.sendResponseHeaders(403, -1);
                    return;
                }
            }
            finally {
                Closeables.close(body, true);
            }
        }
    };

    public void run() throws Exception {
        InetSocketAddress addr = new InetSocketAddress(8080);
        HttpServer server = HttpServer.create(addr, 1 /* backlog */);
        server.createContext(SEARCH_CONTEXT, searchHandler);
        server.createContext(FETCH_CONTEXT, fetchHandler);
        server.setExecutor(Executors.newSingleThreadExecutor());
        server.start();        
        if (true) while (true) Thread.sleep(1000);
    }

    /**
     * @param args
     * @throws IOException 
     * @throws ParseException 
     * @throws MessagingException 
     * @throws InterruptedException 
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            log.error("usage: <index-parent-dir>");
            exit(2);
        }

        new Searcher(new File(args[0], "index")).run();

        shutdownLogger();
    }
}
