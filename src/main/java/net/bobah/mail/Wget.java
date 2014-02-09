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

import static net.bobah.mail.Utils.shutdownLogger;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.StandardOpenOption;
import java.util.Collection;

import net.htmlparser.jericho.Source;
import net.htmlparser.jericho.TextExtractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple URL downloader. WIP.
 * Sample usage in {@link #main(String[])}
 * 
 */
class Wget {
    private static final Logger log = LoggerFactory.getLogger(Wget.class);
    private Wget() {};

    private static final void fetch(URL url, File dest) throws IOException {
        ReadableByteChannel in = null;
        FileChannel out = null;

        try {
            if (!dest.getParentFile().exists()) {
                dest.getParentFile().mkdirs();
            }
            out = FileChannel.open(dest.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);

            in = Channels.newChannel(url.openStream());
            log.info("connected to {}", url);

            long pos = 0l, len = 0l;
            while ((len = out.transferFrom(in, pos, 1l << 20)) > 0) {
                pos += len;
            }
            log.info("written {} -> {}, {}", url, dest, Utils.formatBytes(pos));
        } finally {
            try { if (in != null) in.close(); } catch (IOException notused) { }
            try { if (out != null) out.close(); } catch (IOException notused) { }
        }
    }

    public static void main(String[] args) throws Exception {
        final File localDir = new File("bobah.net/d4d");
        localDir.mkdirs();

        fetch(new URL("http://www.bobah.net/book/export/html/13"), new File(localDir, "bobah.txt"));
        final Collection<File> files = Utils.findFiles(localDir, "txt");
        for(File file: files) {
            System.out.println(new TextExtractor(new Source(file.toURI().toURL())).toString());
        }
        shutdownLogger();
    }

}
