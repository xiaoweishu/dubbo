/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.common.threadpool;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

/**
 * The most important difference between this Executor and other normal Executor is that this one doesn't manage
 * any thread.
 * <p>
 * Tasks submitted to this executor through {@link #execute(Runnable)} will not get scheduled to a specific thread, though normal executors always do the schedule.
 * Those tasks are stored in a blocking queue and will only be executed when a thread calls {@link #waitAndDrain()}, the thread executing the task
 * is exactly the same as the one calling waitAndDrain.
 * 需要注意点是：该类啥时候会被使用
 */
public class ThreadlessExecutor extends AbstractExecutorService {
    private static final Logger logger = LoggerFactory.getLogger(ThreadlessExecutor.class.getName());
    /**
     * 堵塞队列queue(用来在IO线程和业务线程之间传递响应任务),存储响应任务，最终会将响应任务交给等待的业务线程处理
     */
    private final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
    /**
     * ThreadlessExecutor底层关联的共享线程池，主要用于当业务线程不再等待响应时，会由该线程池处理任务，执行任务的与调用 execute() 方法的不是同一个线程
     */
    private ExecutorService sharedExecutor;
    /**
     * 指向请求对应的 DefaultFuture
     */
    private CompletableFuture<?> waitingFuture;
    /**
     * finished 和 waiting 字段控制着等待任务的处理
     */
    private boolean finished = false;
    /**
     * the caller thread（业务线程） is still waiting？
     */
    private volatile boolean waiting = true;

    private final Object lock = new Object();

    public ThreadlessExecutor(ExecutorService sharedExecutor) {
        this.sharedExecutor = sharedExecutor;
    }

    /**
     * 目前没有其他类使用
     *
     * @return
     */
    public CompletableFuture<?> getWaitingFuture() {
        return waitingFuture;
    }

    public void setWaitingFuture(CompletableFuture<?> waitingFuture) {
        this.waitingFuture = waitingFuture;
    }

    public boolean isWaiting() {
        return waiting;
    }

    /**
     * Waits until there is a task, executes the task and all queued tasks (if there're any). The task is either a normal
     * response or a timeout response.
     * 该方法一般与一次 RPC 调用绑定，只会执行一次(对于同步调用，ThreadlessExecutore 每次都会创建)。存储在阻塞队列中的任务，只有当线程调用该方法时才会执行，
     * 重要：执行任务的与调用 waitAndDrain() 方法的是同一个线程
     */
    public void waitAndDrain() throws InterruptedException {
        /**
         * Usually, {@link #waitAndDrain()} will only get called once. It blocks for the response for the first time,
         * once the response (the task) reached and being executed waitAndDrain will return, the whole request process
         * then finishes. Subsequent calls on {@link #waitAndDrain()} (if there're any) should return immediately.
         *
         * There's no need to worry that {@link #finished} is not thread-safe. Checking and updating of
         * 'finished' only appear in waitAndDrain, since waitAndDrain is binding to one RPC call (one thread), the call
         * of it is totally sequential.
         */
        if (finished) {
            return;
        }
        // 取出头部任务，如果没有任务，当前线程会堵塞
        Runnable runnable = queue.take();

        synchronized (lock) {
            waiting = false;
            runnable.run();
        }
        // 取出头部任务，如果没有任务，直接返回null，不会堵塞线程
        runnable = queue.poll();
        while (runnable != null) {
            try {
                runnable.run();
            } catch (Throwable t) {
                logger.info(t);

            }
            runnable = queue.poll();
        }
        // mark the status of ThreadlessExecutor as finished，完成后无业务线程等待
        finished = true;
    }

    public long waitAndDrain(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
        throw new UnsupportedOperationException();
    }

    /**
     * If the calling thread（业务线程） is still waiting for a callback task, add the task into the blocking queue to wait for schedule.
     * Otherwise, submit to shared callback executor directly.
     * 细节：线程池的execute方法重写
     * 收到响应时，IO线程生成一个任务执行，此时代表应该返回响应结果了（已超时，出现异常，或者正常结果）
     *
     * @param runnable
     */
    @Override
    public void execute(Runnable runnable) {
        synchronized (lock) {
            if (!waiting) {
                sharedExecutor.execute(runnable);
            } else {
                // 业务线程还在等待，则将任务写入队列，最终由业务线程自己执行（业务线程在 waitAndDrain 方法上等待任务）
                queue.add(runnable);
            }
        }
    }

    /**
     * tells the thread blocking on {@link #waitAndDrain()} to return, despite of the current status, to avoid endless waiting.
     * 持有waitingFuture的意义
     */
    public void notifyReturn(Throwable t) {
        // an empty runnable task.
        execute(() -> {
            waitingFuture.completeExceptionally(t);
        });
    }

    /**
     * The following methods are still not supported
     */

    @Override
    public void shutdown() {
        shutdownNow();
    }

    @Override
    public List<Runnable> shutdownNow() {
        notifyReturn(new IllegalStateException("Consumer is shutting down and this call is going to be stopped without " +
                "receiving any result, usually this is called by a slow provider instance or bad service implementation."));
        return Collections.emptyList();
    }

    @Override
    public boolean isShutdown() {
        return false;
    }

    @Override
    public boolean isTerminated() {
        return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }
}
