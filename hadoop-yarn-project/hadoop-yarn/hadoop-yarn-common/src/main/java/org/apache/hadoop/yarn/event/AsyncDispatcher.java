/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.event;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.metrics.EventTypeMetrics;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * Dispatches {@link Event}s in a separate thread. Currently only single thread
 * does that. Potentially there could be multiple channels for each event type
 * class and a thread pool can be used to dispatch the events.
 */
@SuppressWarnings("rawtypes")
@Public
@Evolving
public class AsyncDispatcher extends AbstractService implements Dispatcher {

    private static final Logger LOG =
            LoggerFactory.getLogger(AsyncDispatcher.class);
    private static final Marker FATAL =
            MarkerFactory.getMarker("FATAL");
    // todo 待调度处理事件阻塞队列
    // todo 调用有参构造函数的时候初始化，传入线程安全的链式阻塞队列LinkedBlockingQueue实例
    private final BlockingQueue<Event> eventQueue;
    private volatile int lastEventQueueSizeLogged = 0;
    private volatile int lastEventDetailsQueueSizeLogged = 0;
    // todo AsyncDispatcher是否停止的标志位
    private volatile boolean stopped = false;

    //Configuration for control the details queue event printing.
    private int detailsInterval;
    private boolean printTrigger = false;

    // Configuration flag for enabling/disabling draining dispatcher's events on
    // stop functionality.
    // todo 在stop功能中开启/禁用流尽分发器事件的配置标志位
    private volatile boolean drainEventsOnStop = false;

    // Indicates all the remaining dispatcher's events on stop have been drained
    // and processed.
    // Race condition happens if dispatcher thread sets drained to true between
    // handler setting drained to false and enqueueing event. YARN-3878 decided
    // to ignore it because of its tiny impact. Also see YARN-5436.
    // todo stop功能中所有剩余分发器事件已经被处理或流尽的标志位
    private volatile boolean drained = true;
    // todo drained的等待锁
    private final Object waitForDrained = new Object();

    // For drainEventsOnStop enabled only, block newly coming events into the
    // queue while stopping.
    // todo 在AsyncDispatcher停止过程中阻塞新近到来的事件进入队列的标志位，仅当drainEventsOnStop启用（即为true）时有效
    private volatile boolean blockNewEvents = false;
    // todo 事件处理器实例 向队列中添加事件，生产者
    private final EventHandler<Event> handlerInstance = new GenericEventHandler();
    //todo 事件处理线程
    private Thread eventHandlingThread;
    protected final Map<Class<? extends Enum>, EventHandler> eventDispatchers;
    // todo 标志位：确保调度程序崩溃，但不做系统退出system-exit
    private boolean exitOnDispatchException = true;
    // todo  类型为: HashMap<Class<? extends Enum>, EventHandler>();
    private Map<Class<? extends Enum>,
            EventTypeMetrics> eventTypeMetricsMap;

    private Clock clock = new MonotonicClock();

    private ThreadPoolExecutor printEventDetailsExecutor;

    /**
     * The thread name for dispatcher.
     */
    private String dispatcherThreadName = "AsyncDispatcher event handler";

    public AsyncDispatcher() {
        this(new LinkedBlockingQueue<Event>());
    }

    public AsyncDispatcher(BlockingQueue<Event> eventQueue) {
        super("Dispatcher");
        this.eventQueue = eventQueue;
        this.eventDispatchers = new HashMap<Class<? extends Enum>, EventHandler>();
        this.eventTypeMetricsMap = new HashMap<Class<? extends Enum>,
                EventTypeMetrics>();
    }

    /**
     * Set a name for this dispatcher thread.
     *
     * @param dispatcherName name of the dispatcher thread
     */
    public AsyncDispatcher(String dispatcherName) {
        this();
        dispatcherThreadName = dispatcherName;
    }

    Runnable createThread() {
        return new Runnable() {
            @Override
            public void run() {
                //todo 如果不是停止, 或者当前线程不被中断.
                while (!stopped && !Thread.currentThread().isInterrupted()) {
                    //todo  判断事件调度队列eventQueue是否为空，并赋值给标志位drained
                    drained = eventQueue.isEmpty();
                    // blockNewEvents is only set when dispatcher is draining to stop,
                    // adding this check is to avoid the overhead of acquiring the lock
                    // and calling notify every time in the normal run of the loop.
                    // todo 如果停止过程中阻止新的事件加入待处理队列，即标志位blockNewEvents为true
                    if (blockNewEvents) {
                        //todo 在这里面有锁
                        synchronized (waitForDrained) {
                            if (drained) {
                                // todo 如果待处理队列中的事件都已调度完毕，调用waitForDrained的notify()方法通知等待者
                                waitForDrained.notify();
                            }
                        }
                    }
                    Event event;
                    try {
                        //todo 获取事件
                        // todo 从事件调度队列eventQueue中取出一个事件
                        // todo take()方法为取走BlockingQueue里排在首位的对象，若BlockingQueue为空，阻塞进入等待状态直到  BlockingQueue有新的数据被加入
                        event = eventQueue.take();
                    } catch (InterruptedException ie) {
                        if (!stopped) {
                            LOG.warn("AsyncDispatcher thread interrupted", ie);
                        }
                        return;
                    }
                    // todo 如果取出待处理事件event，即不为null
                    if (event != null) {
                        if (eventTypeMetricsMap.
                                get(event.getType().getDeclaringClass()) != null) {
                            long startTime = clock.getTime();
                            //todo 调度事件event 调用dispatch()方法进行分发
                            dispatch(event);
                            eventTypeMetricsMap.get(event.getType().getDeclaringClass())
                                    .increment(event.getType(),
                                            clock.getTime() - startTime);
                        } else {
                            dispatch(event);
                        }
                        if (printTrigger) {
                            //Log the latest dispatch event type
                            // may cause the too many events queued
                            LOG.info("Latest dispatch event type: " + event.getType());
                            printTrigger = false;
                        }
                    }
                }
            }
        };
    }

    @VisibleForTesting
    public void disableExitOnDispatchException() {
        exitOnDispatchException = false;
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        super.serviceInit(conf);
        this.detailsInterval = getConfig().getInt(YarnConfiguration.
                        YARN_DISPATCHER_PRINT_EVENTS_INFO_THRESHOLD,
                YarnConfiguration.
                        DEFAULT_YARN_DISPATCHER_PRINT_EVENTS_INFO_THRESHOLD);

        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("PrintEventDetailsThread #%d")
                .build();
        // Thread pool for async print event details,
        // to prevent wasting too much time for RM.
        int numCorePoolSizeThreads = getConfig().getInt(
                YarnConfiguration.YARN_DISPATCHER_PRINT_THREAD_POOL_CORE_POOL_SIZE,
                YarnConfiguration.DEFAULT_YARN_DISPATCHER_PRINT_THREAD_POOL_CORE_POOL_SIZE);

        int numMaximumPoolSizeThreads = getConfig().getInt(
                YarnConfiguration.YARN_DISPATCHER_PRINT_THREAD_POOL_MAXIMUM_POOL_SIZE,
                YarnConfiguration.DEFAULT_YARN_DISPATCHER_PRINT_THREAD_POOL_MAXIMUM_POOL_SIZE);

        long keepAliveTime =
                conf.getTimeDuration(YarnConfiguration.YARN_DISPATCHER_PRINT_THREAD_POOL_KEEP_ALIVE_TIME,
                        YarnConfiguration.DEFAULT_YARN_DISPATCHER_PRINT_THREAD_POOL_KEEP_ALIVE_TIME,
                        TimeUnit.SECONDS);

        printEventDetailsExecutor = new ThreadPoolExecutor(
                numCorePoolSizeThreads, numMaximumPoolSizeThreads, keepAliveTime, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(), threadFactory);
    }

    @Override
    protected void serviceStart() throws Exception {
        //start all the components
        super.serviceStart();
        // todo 创建事件处理调度线程 eventHandlingThread 消费者
        eventHandlingThread = new Thread(createThread());
        // todo 设置线程名为AsyncDispatcher event handler
        eventHandlingThread.setName(dispatcherThreadName);
        // todo 启动事件处理调度线程eventHandlingThread
        eventHandlingThread.start();
    }

    public void setDrainEventsOnStop() {
        drainEventsOnStop = true;
    }

    @Override
    protected void serviceStop() throws Exception {
        if (drainEventsOnStop) {
            blockNewEvents = true;
            LOG.info("AsyncDispatcher is draining to stop, ignoring any new events.");
            long endTime = System.currentTimeMillis() + getConfig()
                    .getLong(YarnConfiguration.DISPATCHER_DRAIN_EVENTS_TIMEOUT,
                            YarnConfiguration.DEFAULT_DISPATCHER_DRAIN_EVENTS_TIMEOUT);

            synchronized (waitForDrained) {
                while (!isDrained() && eventHandlingThread != null
                        && eventHandlingThread.isAlive()
                        && System.currentTimeMillis() < endTime) {
                    waitForDrained.wait(100);
                    LOG.info("Waiting for AsyncDispatcher to drain. Thread state is :" +
                            eventHandlingThread.getState());
                }
            }
        }
        stopped = true;
        if (eventHandlingThread != null) {
            eventHandlingThread.interrupt();
            try {
                eventHandlingThread.join();
            } catch (InterruptedException ie) {
                LOG.warn("Interrupted Exception while stopping", ie);
            }
        }
        printEventDetailsExecutor.shutdownNow();

        // stop all the components
        super.serviceStop();
    }

    @SuppressWarnings("unchecked")
    protected void dispatch(Event event) {
        //all events go thru this loop
        LOG.debug("Dispatching the event {}.{}", event.getClass().getName(),
                event);
        // todo 根据事件event获取事件类型枚举类type
        Class<? extends Enum> type = event.getType().getDeclaringClass();

        try {
            //todo 获取事件类型所对应的Handler
            EventHandler handler = eventDispatchers.get(type);
            if (handler != null) {
                //todo 调用对应的 handler 来处理事件.
                handler.handle(event);
            } else {
                // todo 否则抛出异常，提示针对事件类型type的事件处理器handler没有注册
                throw new Exception("No handler for registered for " + type);
            }
        } catch (Throwable t) {
            //TODO Maybe log the state of the queue
            LOG.error(FATAL, "Error in dispatcher thread", t);
            // If serviceStop is called, we should exit this thread gracefully.
            if (exitOnDispatchException
                    && (ShutdownHookManager.get().isShutdownInProgress()) == false
                    && stopped == false) {
                stopped = true;
                Thread shutDownThread = new Thread(createShutDownThread());
                shutDownThread.setName("AsyncDispatcher ShutDown handler");
                shutDownThread.start();
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void register(Class<? extends Enum> eventType,
                         EventHandler handler) {
        /* check to see if we have a listener registered */
        EventHandler<Event> registeredHandler = (EventHandler<Event>)
                eventDispatchers.get(eventType);
        LOG.info("Registering " + eventType + " for " + handler.getClass());
        if (registeredHandler == null) {
            eventDispatchers.put(eventType, handler);
        } else if (!(registeredHandler instanceof MultiListenerHandler)) {
            /* for multiple listeners of an event add the multiple listener handler */
            MultiListenerHandler multiHandler = new MultiListenerHandler();
            multiHandler.addHandler(registeredHandler);
            multiHandler.addHandler(handler);
            eventDispatchers.put(eventType, multiHandler);
        } else {
            /* already a multilistener, just add to it */
            MultiListenerHandler multiHandler
                    = (MultiListenerHandler) registeredHandler;
            multiHandler.addHandler(handler);
        }
    }

    @Override
    public EventHandler<Event> getEventHandler() {
        return handlerInstance;
    }

    //todo 默认的通用事件处理 --产生数据
    class GenericEventHandler implements EventHandler<Event> {
        private void printEventQueueDetails() {
            Iterator<Event> iterator = eventQueue.iterator();
            Map<Enum, Long> counterMap = new HashMap<>();
            while (iterator.hasNext()) {
                Enum eventType = iterator.next().getType();
                if (!counterMap.containsKey(eventType)) {
                    counterMap.put(eventType, 0L);
                }
                counterMap.put(eventType, counterMap.get(eventType) + 1);
            }
            for (Map.Entry<Enum, Long> entry : counterMap.entrySet()) {
                long num = entry.getValue();
                LOG.info("Event type: " + entry.getKey()
                        + ", Event record counter: " + num);
            }
        }

        public void handle(Event event) {
            // todo 如果blockNewEvents为true，即AsyncDispatcher服务停止过程正在发生，
            // todo 且阻止新的事件加入待调度处理事件队列eventQueue，直接返回
            if (blockNewEvents) {
                return;
            }
            // todo 标志位drained设置为false，说明队列中尚有事件需要调度
            drained = false;

            /* all this method does is enqueue all the events onto the queue */
            // todo 获取队列eventQueue大小qSize
            int qSize = eventQueue.size();
            // todo 每隔1000记录一条info级别日志信息，比如：Size of event-queue is 2000
            if (qSize != 0 && qSize % 1000 == 0
                    && lastEventQueueSizeLogged != qSize) {
                lastEventQueueSizeLogged = qSize;
                LOG.info("Size of event-queue is " + qSize);
            }
            if (qSize != 0 && qSize % detailsInterval == 0
                    && lastEventDetailsQueueSizeLogged != qSize) {
                lastEventDetailsQueueSizeLogged = qSize;
                printEventDetailsExecutor.submit(this::printEventQueueDetails);
                printTrigger = true;
            }
            // todo 获取队列eventQueue剩余容量remCapacity
            int remCapacity = eventQueue.remainingCapacity();
            // todo 如果剩余容量remCapacity小于1000，记录warn级别日志信息，
            //  比如:Very low remaining capacity in the event-queue:  888
            if (remCapacity < 1000) {
                LOG.warn("Very low remaining capacity in the event-queue: "
                        + remCapacity);
            }
            try {
                // todo 队列eventQueue中添加事件event
                eventQueue.put(event);
            } catch (InterruptedException e) {
                if (!stopped) {
                    LOG.warn("AsyncDispatcher thread interrupted", e);
                }
                // Need to reset drained flag to true if event queue is empty,
                // otherwise dispatcher will hang on stop.
                drained = eventQueue.isEmpty();
                throw new YarnRuntimeException(e);
            }
        }

        ;
    }

    /**
     * Multiplexing an event. Sending it to different handlers that
     * are interested in the event.
     *
     * @param //<T> the type of event these multiple handlers are interested in.
     */
    static class MultiListenerHandler implements EventHandler<Event> {
        List<EventHandler<Event>> listofHandlers;

        public MultiListenerHandler() {
            listofHandlers = new ArrayList<EventHandler<Event>>();
        }

        @Override
        public void handle(Event event) {
            for (EventHandler<Event> handler : listofHandlers) {
                handler.handle(event);
            }
        }

        void addHandler(EventHandler<Event> handler) {
            listofHandlers.add(handler);
        }

    }

    Runnable createShutDownThread() {
        return new Runnable() {
            @Override
            public void run() {
                LOG.info("Exiting, bbye..");
                System.exit(-1);
            }
        };
    }

    @VisibleForTesting
    protected boolean isEventThreadWaiting() {
        return eventHandlingThread.getState() == Thread.State.WAITING;
    }

    protected boolean isDrained() {
        return drained;
    }

    protected boolean isStopped() {
        return stopped;
    }

    public void addMetrics(EventTypeMetrics metrics,
                           Class<? extends Enum> eventClass) {
        eventTypeMetricsMap.put(eventClass, metrics);
    }

    public int getEventQueueSize() {
        return eventQueue.size();
    }
}
