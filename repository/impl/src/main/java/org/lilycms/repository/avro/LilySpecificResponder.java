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
package org.lilycms.repository.avro;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.ipc.Responder;
import org.apache.avro.specific.*;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

// This code is copied from the Avro codebase with some customisations to handle
// remote propagation of runtime exceptions

public class LilySpecificResponder extends Responder {
  private Object impl;
  private SpecificData data;
    private AvroConverter converter;

  public LilySpecificResponder(Class iface, Object impl, AvroConverter converter) {
    this(SpecificData.get().getProtocol(iface), impl);
      this.converter = converter;
  }

  public LilySpecificResponder(Protocol protocol, Object impl) {
    this(protocol, impl, SpecificData.get());
  }

  protected LilySpecificResponder(Protocol protocol, Object impl,
                              SpecificData data) {
    super(protocol);
    this.impl = impl;
    this.data = data;
  }

  protected DatumWriter<Object> getDatumWriter(Schema schema) {
    return new SpecificDatumWriter<Object>(schema);
  }

  protected DatumReader<Object> getDatumReader(Schema schema) {
    return new SpecificDatumReader<Object>(schema);
  }

  @Override
  public Object readRequest(Schema schema, Decoder in) throws IOException {
    Object[] args = new Object[schema.getFields().size()];
    int i = 0;
    for (Schema.Field param : schema.getFields())
      args[i++] = getDatumReader(param.schema()).read(null, in);
    return args;
  }

  @Override
  public void writeResponse(Schema schema, Object response, Encoder out)
    throws IOException {
    getDatumWriter(schema).write(response, out);
  }

  @Override
  public void writeError(Schema schema, Object error,
                         Encoder out) throws IOException {
      if (!(error instanceof SpecificRecord)) {
          System.out.println("-----------------------------------------------");
          System.out.println("Avro responder is being asked to write an exception which is not a specific Avro type.");
          System.out.println("ClassName: " + error.getClass().getName());
          System.out.println("toString: " + error.toString());

          if (error instanceof Throwable) {
              ((Throwable)error).printStackTrace();
          }
          System.out.println("-----------------------------------------------");
      }

    getDatumWriter(schema).write(error, out);
  }

    @Override
    public Object respond(Protocol.Message message, Object request) throws Exception {
      Class[] paramTypes = new Class[message.getRequest().getFields().size()];
      int i = 0;
      try {
        for (Schema.Field param: message.getRequest().getFields())
          paramTypes[i++] = data.getClass(param.schema());
        Method method = impl.getClass().getMethod(message.getName(), paramTypes);
        return method.invoke(impl, (Object[])request);
      } catch (InvocationTargetException e) {
          if (e.getTargetException() instanceof SpecificRecord) {
              throw (Exception)e.getTargetException();
          } else {
              throw converter.convertOtherException(e.getTargetException());
          }
      } catch (NoSuchMethodException e) {
        throw new AvroRuntimeException(e);
      } catch (IllegalAccessException e) {
        throw new AvroRuntimeException(e);
      }
    }
}
