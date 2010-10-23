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
package org.lilyproject.util;

public class Pair<T1, T2> {
    private T1 v1;
    private T2 v2;

    public Pair(T1 v1, T2 v2) {
        this.v1 = v1;
        this.v2 = v2;
    }

    public T1 getV1() {
        return v1;
    }

    public T2 getV2() {
        return v2;
    }

    @Override
    public int hashCode() {
        int result = 17;
        if (v1 != null)
            result = 37 * result + v1.hashCode();
        if (v2 != null)
            result = 37 * result + v2.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Pair) {
            Pair other = (Pair)obj;
            return ObjectUtils.safeEquals(v1, other.v1) && ObjectUtils.safeEquals(v2, other.v2);
        }
        return false;
    }
}
