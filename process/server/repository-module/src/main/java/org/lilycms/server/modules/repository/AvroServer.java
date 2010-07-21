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

import org.apache.avro.ipc.HttpServer;
import org.apache.avro.ipc.Responder;
import org.lilycms.repository.api.Repository;
import org.lilycms.repository.avro.*;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;

public class AvroServer {
    private String bindAddress;
    private Repository repository;
    private int port;

    private HttpServer server;

    public AvroServer(String bindAddress, Repository repository, int port) {
        this.bindAddress = bindAddress;
        this.repository = repository;
        this.port = port;
    }

    @PostConstruct
    public void start() throws IOException {
        AvroConverter avroConverter = new AvroConverter();
        avroConverter.setRepository(repository);

        AvroLilyImpl avroLily = new AvroLilyImpl(repository, avroConverter);
        Responder responder = new LilySpecificResponder(AvroLily.class, avroLily, avroConverter);
        server = new HttpServer(responder, port);
    }
    
    @PreDestroy
    public void stop() {
        server.close();
    }

    public int getPort() {
        return server.getPort();
    }
}
