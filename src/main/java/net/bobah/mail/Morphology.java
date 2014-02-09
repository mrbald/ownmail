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

import java.util.List;

import org.apache.lucene.morphology.russian.RussianMorphology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A sandbox to research the subject of indexing non-trivial languages  
 * with grammatical cases and diclensions. WIP.
 *
 */
public class Morphology {
    private static final Logger log = LoggerFactory.getLogger(Morphology.class);

    /**
     * @param args
     * @throws InterruptedException 
     * @throws Exception 
     */
    public static void main(String[] args) {
        RussianMorphology morph;
        try {
            morph = new RussianMorphology();
            List<String> forms = morph.getMorphInfo("запердоль-ка");
            log.info("MorphInfo: {}", forms.toString());
        } catch (final Throwable e) {
            log.error("exception", e);
        }
        
        shutdownLogger();
    }

}
