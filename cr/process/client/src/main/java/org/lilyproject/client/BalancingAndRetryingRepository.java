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
package org.lilyproject.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.impl.IdGeneratorImpl;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * Creates a proxy around Repository and TypeManager that automatically balances requests
 * over different Lily nodes, and can optionally retry operations when they fail due to
 * IO related exceptions or when no Lily servers are available.
 */
public class BalancingAndRetryingRepository {
    public static Repository getInstance(LilyClient lilyClient) {

        InvocationHandler typeManagerHandler = new TypeManagerInvocationHandler(lilyClient);
        TypeManager typeManager = (TypeManager)Proxy.newProxyInstance(TypeManager.class.getClassLoader(),
                new Class[] { TypeManager.class }, typeManagerHandler);

        InvocationHandler repositoryHandler = new RepositoryInvocationHandler(lilyClient, typeManager);
        Repository repository = (Repository)Proxy.newProxyInstance(Repository.class.getClassLoader(),
                new Class[] { Repository.class }, repositoryHandler);

        return repository;
    }

    private static final class TypeManagerInvocationHandler extends RetryBase implements InvocationHandler {
        private final LilyClient lilyClient;

        public TypeManagerInvocationHandler(LilyClient lilyClient) {
            super(lilyClient.getRetryConf());
            this.lilyClient = lilyClient;
        }

        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            long startedAt = System.currentTimeMillis();
            int attempt = 0;

            while (true) {
                try {
                    TypeManager typeManager = lilyClient.getPlainRepository().getTypeManager();
                    return method.invoke(typeManager, args);
                } catch (NoServersException e) {
                    // Needs to be wrapped because NoServersException is not in the throws clause of the
                    // Repository & TypeManager methods
                    handleThrowable(new IOTypeException(e), method, startedAt, attempt, OperationType.TYPE);
                } catch (Throwable throwable) {
                    handleThrowable(throwable, method, startedAt, attempt, OperationType.TYPE);
                }
                attempt++;
            }
        }
    }

    private static final class RepositoryInvocationHandler extends RetryBase implements InvocationHandler {
        private final LilyClient lilyClient;
        private final TypeManager typeManager;
        private final IdGenerator idGenerator = new IdGeneratorImpl();

        private RepositoryInvocationHandler(LilyClient lilyClient, TypeManager typeManager) {
            super(lilyClient.getRetryConf());
            this.lilyClient = lilyClient;
            this.typeManager = typeManager;
        }

        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.getName().equals("getTypeManager")) {
                return typeManager;
            } else if (method.getName().equals("getIdGenerator")) {
                // this method is here so that we would not go into the retry loop, which would throw an
                // RetriesExhausted checked exception, which would be a bit strange to declare for the
                // getIdGenerator method
                return idGenerator;
            }

            long startedAt = System.currentTimeMillis();
            int attempt = 0;

            while (true) {
                try {
                    Repository repository = lilyClient.getPlainRepository();
                    return method.invoke(repository, args);
                } catch (NoServersException e) {
                    // Needs to be wrapped because NoServersException is not in the throws clause of the
                    // Repository & TypeManager methods
                    if (isBlobMethod(method)) {
                        handleThrowable(new IOBlobException(e), method, startedAt, attempt, OperationType.BLOB);
                    } else {
                        handleThrowable(new IORecordException(e), method, startedAt, attempt, OperationType.RECORD);
                    }
                } catch (Throwable throwable) {
                    if (isBlobMethod(method)) {
                        handleThrowable(throwable, method, startedAt, attempt, OperationType.BLOB);
                    } else {
                        handleThrowable(throwable, method, startedAt, attempt, OperationType.RECORD);
                    }
                }

                attempt++;
            }
        }

        private boolean isBlobMethod(Method method) {
            if (method.getName().equals("delete")) {
                Class[] params = method.getParameterTypes();
                return params.length == 1 && Blob.class.isAssignableFrom(params[0].getClass());
            }
            return false;
        }
    }

    private enum OperationType { RECORD, TYPE, BLOB };

    private static class RetryBase {
        private Log log = LogFactory.getLog(getClass());
        private RetryConf retryConf;

        protected RetryBase(RetryConf retryConf) {
            this.retryConf = retryConf;
        }

        protected void handleThrowable(Throwable throwable, Method method, long startedAt, int attempt,
                OperationType opType) throws Throwable {

            if (throwable instanceof InvocationTargetException)
                throwable = ((InvocationTargetException)throwable).getTargetException();

            if (throwable instanceof IORecordException || throwable instanceof IOBlobException ||
                    throwable instanceof IOTypeException) {

                boolean callInitiated = true;
                if (throwable.getCause() instanceof NoServersException) {
                    // I initially thought we could also assume the request was not yet launched in case of
                    // ConnectException with msg "Connection refused". However, at least with the Avro HttpTransceiver,
                    // this exception can also occur when the connection is lost between writing the request
                    // and reading the response. On reading the response, the Java URLConnection will see
                    // there is no connection anymore and reestablish it, hence giving a "connection refused" error.
                    // In this situation, the request is sent out by the server, so it is not safe to simply redo it.
                    callInitiated = false;
                }
                handleRetry(method, startedAt, attempt, callInitiated, throwable, opType);
            } else {
                throw throwable;
            }
        }

        protected void handleRetry(Method method, long startedAt, int attempt,
                boolean callInitiated, Throwable throwable, OperationType opType) throws Throwable {

            long timeSpentRetrying = System.currentTimeMillis() - startedAt;
            if (timeSpentRetrying > retryConf.getRetryMaxTime()) {
                switch (opType) {
                    case RECORD:
                        throw new RetriesExhaustedRecordException(getOpString(method), attempt, timeSpentRetrying, throwable);
                    case TYPE:
                        throw new RetriesExhaustedTypeException(getOpString(method), attempt, timeSpentRetrying, throwable);
                    case BLOB:
                        throw new RetriesExhaustedBlobException(getOpString(method), attempt, timeSpentRetrying, throwable);
                    default:
                        throw new RuntimeException("This should never occur: unhandled op type: " + opType);
                }
            }

            String methodName = method.getName();

            boolean retry = false;

            // Since the "newSomething" methods are simple factory methods, put them in the same class as reads
            // TODO: the methods starting with get include the blob methods getInputStream and getOutputStream,
            //       which should probably have a different treatment
            if ((methodName.startsWith("read") || methodName.startsWith("get") || methodName.startsWith("new"))
                    && retryConf.getRetryReads()) {
                retry = true;
            } else if (methodName.equals("createOrUpdate") && retryConf.getRetryCreateOrUpdate()) {
                retry = true;
            } else if (methodName.startsWith("update") && retryConf.getRetryUpdates()) {
                retry = true;
            } else if (methodName.startsWith("delete") && retryConf.getRetryDeletes()) {
                retry = true;
            } else if (methodName.startsWith("create") && retryConf.getRetryCreate() && (!callInitiated || retryConf.getRetryCreateRiskDoubles())) {
                retry = true;
            }

            if (retry) {
                int sleepTime = getSleepTime(attempt);
                if (log.isDebugEnabled() || log.isInfoEnabled()) {
                    String message = "Sleeping " + sleepTime + "ms before retrying operation " +
                            getOpString(method) + " attempt " + attempt +
                            " failed due to " + throwable.getCause().toString();
                    if (log.isDebugEnabled()) {
                        log.debug(message, throwable);
                    } else if (log.isInfoEnabled()) {
                        log.info(message);
                    }
                }
                Thread.sleep(sleepTime);
            } else {
                throw throwable;
            }
        }

        private int getSleepTime(int attempt) throws InterruptedException {
            int pos = attempt < retryConf.getRetryIntervals().length ? attempt : retryConf.getRetryIntervals().length - 1;
            int waitTime = retryConf.getRetryIntervals()[pos];
            return waitTime;
        }

        private String getOpString(Method method) {
            return method.getDeclaringClass().getSimpleName() + "." + method.getName();
        }
    }
}
