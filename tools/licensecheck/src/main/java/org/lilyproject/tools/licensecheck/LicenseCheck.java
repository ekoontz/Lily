/*
 * Copyright 2010 Outerthought bvba
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.tools.licensecheck;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Set;

public class LicenseCheck {
    private static Set extensions = new HashSet();
    static {
        extensions.add("java");
        extensions.add("xsl");
        extensions.add("js");
        extensions.add("xml");
    }
    private static Set exclusions = new HashSet();
    static {
        exclusions.add("target");
        exclusions.add(".svn");
    }

    public static void main(String[] args) throws Exception {
        new LicenseCheck().run();
    }

    public void run() throws Exception {
        File file = new File(".");
        System.out.println("Will print names of files with missing license header, if any.");
        checkRecursive(file);
    }

    private void checkRecursive(File directory) throws Exception {
        File[] files = directory.listFiles();
        for (int i = 0; i < files.length; i++) {
            File file = files[i];
            if (!exclusions.contains(file.getName())) {
                if (file.isDirectory()) {
                    checkRecursive(file);
                } else {
                    String name = file.getName();
                    int pos = name.lastIndexOf('.');
                    if (pos != -1) {
                        String extension = name.substring(pos + 1);
                        if (extensions.contains(extension)) {
                            if (!hasLicenseHeader(file)) {
                                System.out.println(file.getAbsolutePath());
                            }
                        }
                    }
                }
            }
        }
    }

    private boolean hasLicenseHeader(File file) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        try {
            String line;
            int count = 0;
            while ((line = reader.readLine()) != null) {
                if (line.indexOf("Licensed under the Apache License") != -1
                    || line.indexOf("Do not apply Lily license") != -1)
                    return true;
                count++;
                if (count > 15)  // read max 15 lines in file
                    break;
            }

            return false;
        } finally {
            reader.close();
        }
    }
}
