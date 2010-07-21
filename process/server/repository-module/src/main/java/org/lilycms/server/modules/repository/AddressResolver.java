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
package org.lilycms.server.modules.repository;

import org.apache.hadoop.net.DNS;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

public class AddressResolver {
    private String interface_;
    private String nameserver;
    
    private String hostName;
    private String hostAddress;

    public AddressResolver(String interface_, String nameserver) throws UnknownHostException {
        hostName = DNS.getDefaultHost(interface_, nameserver);
        hostAddress = new InetSocketAddress(hostName, 1).getAddress().getHostAddress();
    }

    public String getHostName() {
        return hostName;
    }

    public String getHostAddress() {
        return hostAddress;
    }
}
