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
package org.lilyproject.indexer.model.indexerconf;

public abstract class BaseValue implements Value {
    private String formatterName;
    private boolean extractContent;

    public BaseValue(boolean extractContent, String formatter) {
        this.extractContent = extractContent;
        this.formatterName = formatter;
    }

    public String getFormatter() {
        return formatterName;
    }

    public boolean extractContent() {
        return extractContent;
    }

}
