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
