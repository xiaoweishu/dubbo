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
package org.apache.dubbo.remoting.exchange.support.header;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.ExecutionException;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.exchange.ExchangeChannel;
import org.apache.dubbo.remoting.exchange.ExchangeHandler;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.Response;
import org.apache.dubbo.remoting.exchange.support.DefaultFuture;
import org.apache.dubbo.remoting.transport.ChannelHandlerDelegate;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletionStage;

import static org.apache.dubbo.common.constants.CommonConstants.READONLY_EVENT;


/**
 * ExchangeReceiver
 * HeaderExchangeHandler 是 Dubbo 框架中用于处理基于头部协议的通信的核心类。
 * 它实现了 ChannelHandlerDelegate 接口，封装了请求、响应和事件的处理逻辑。
 * 主要职责包括：
 * 处理客户端和服务器之间的请求和响应。
 * 支持心跳检测、只读事件等特殊消息。
 * 提供异常处理机制，确保通信的健壮性
 * <p>
 * ---
 * <p>
 * ### 设计思想分析
 * <p>
 * 1. **分层架构**
 * - `HeaderExchangeHandler` 作为通信层的一部分，封装了底层通信细节，提供了统一的接口供上层调用。
 * - 通过 `ChannelHandlerDelegate` 接口，实现了对实际处理器的委托调用，增强了代码的可扩展性和灵活性。
 * <p>
 * 2. **事件驱动模型**
 * - 使用事件驱动的方式处理连接、断开、发送、接收等通信事件，简化了代码逻辑，提高了系统的响应速度。
 * - 每个事件都有明确的处理逻辑，确保了通信的完整性和一致性。
 * <p>
 * 3. **异步处理**
 * - 在处理请求时，使用 `CompletionStage` 实现异步回调，避免阻塞主线程，提升了系统的并发性能。
 * - 异常处理机制完善，确保在异步调用过程中出现的异常能够被正确捕获和处理。
 * <p>
 * 4. **心跳检测与事件处理**
 * - 支持心跳检测，确保长连接的活跃状态。
 * - 特殊事件（如只读事件）通过设置通道属性进行处理，增强了框架的功能性。
 * <p>
 * 5. **线程安全与资源管理**
 * - 使用 `HeaderExchangeChannel` 管理通道，确保多线程环境下的线程安全。
 * - 在通道断开时，主动关闭未完成的请求并清理资源，避免内存泄漏。
 * <p>
 * 6. **日志与调试**
 * - 提供了详细的日志记录功能，便于问题排查和调试。
 * - 对关键操作（如发送、接收、异常处理）进行了日志记录，确保系统运行的透明度。
 * <p>
 * 7. **扩展性与灵活性**
 * - 支持多种消息类型（请求、响应、字符串等），适应不同的业务场景。
 * - 用户可以通过配置文件轻松切换不同的处理器实现，增强了框架的可配置性和扩展性。
 */
public class HeaderExchangeHandler implements ChannelHandlerDelegate {

    protected static final Logger logger = LoggerFactory.getLogger(HeaderExchangeHandler.class);

    private final ExchangeHandler handler;

    public HeaderExchangeHandler(ExchangeHandler handler) {
        if (handler == null) {
            throw new IllegalArgumentException("handler == null");
        }
        this.handler = handler;
    }

    static void handleResponse(Channel channel, Response response) throws RemotingException {
        if (response != null && !response.isHeartbeat()) {
            DefaultFuture.received(channel, response);
        }
    }

    private static boolean isClientSide(Channel channel) {
        InetSocketAddress address = channel.getRemoteAddress();
        URL url = channel.getUrl();
        return url.getPort() == address.getPort() &&
                NetUtils.filterLocalHost(url.getIp())
                        .equals(NetUtils.filterLocalHost(address.getAddress().getHostAddress()));
    }

    void handlerEvent(Channel channel, Request req) throws RemotingException {
        if (req.getData() != null && req.getData().equals(READONLY_EVENT)) {
            channel.setAttribute(Constants.CHANNEL_ATTRIBUTE_READONLY_KEY, Boolean.TRUE);
        }
    }

    void handleRequest(final ExchangeChannel channel, Request req) throws RemotingException {
        Response res = new Response(req.getId(), req.getVersion());
        if (req.isBroken()) {
            Object data = req.getData();

            String msg;
            if (data == null) {
                msg = null;
            } else if (data instanceof Throwable) {
                msg = StringUtils.toString((Throwable) data);
            } else {
                msg = data.toString();
            }
            res.setErrorMessage("Fail to decode request due to: " + msg);
            res.setStatus(Response.BAD_REQUEST);

            channel.send(res);
            return;
        }
        // find handler by message class.
        Object msg = req.getData();
        try {
            CompletionStage<Object> future = handler.reply(channel, msg);
            future.whenComplete((appResult, t) -> {
                try {
                    if (t == null) {
                        res.setStatus(Response.OK);
                        res.setResult(appResult);
                    } else {
                        res.setStatus(Response.SERVICE_ERROR);
                        res.setErrorMessage(StringUtils.toString(t));
                    }
                    channel.send(res);
                } catch (RemotingException e) {
                    logger.warn("Send result to consumer failed, channel is " + channel + ", msg is " + e);
                }
            });
        } catch (Throwable e) {
            res.setStatus(Response.SERVICE_ERROR);
            res.setErrorMessage(StringUtils.toString(e));
            channel.send(res);
        }
    }

    @Override
    public void connected(Channel channel) throws RemotingException {
        ExchangeChannel exchangeChannel = HeaderExchangeChannel.getOrAddChannel(channel);
        handler.connected(exchangeChannel);
    }

    @Override
    public void disconnected(Channel channel) throws RemotingException {
        ExchangeChannel exchangeChannel = HeaderExchangeChannel.getOrAddChannel(channel);
        try {
            handler.disconnected(exchangeChannel);
        } finally {
            DefaultFuture.closeChannel(channel);
            HeaderExchangeChannel.removeChannel(channel);
        }
    }

    @Override
    public void sent(Channel channel, Object message) throws RemotingException {
        Throwable exception = null;
        try {
            ExchangeChannel exchangeChannel = HeaderExchangeChannel.getOrAddChannel(channel);
            handler.sent(exchangeChannel, message);
        } catch (Throwable t) {
            exception = t;
            HeaderExchangeChannel.removeChannelIfDisconnected(channel);
        }
        if (message instanceof Request) {
            Request request = (Request) message;
            DefaultFuture.sent(channel, request);
        }
        if (exception != null) {
            if (exception instanceof RuntimeException) {
                throw (RuntimeException) exception;
            } else if (exception instanceof RemotingException) {
                throw (RemotingException) exception;
            } else {
                throw new RemotingException(channel.getLocalAddress(), channel.getRemoteAddress(),
                        exception.getMessage(), exception);
            }
        }
    }

    @Override
    public void received(Channel channel, Object message) throws RemotingException {
        final ExchangeChannel exchangeChannel = HeaderExchangeChannel.getOrAddChannel(channel);
        if (message instanceof Request) {
            // handle request.
            Request request = (Request) message;
            if (request.isEvent()) {
                handlerEvent(channel, request);
            } else {
                if (request.isTwoWay()) {
                    handleRequest(exchangeChannel, request);
                } else {
                    handler.received(exchangeChannel, request.getData());
                }
            }
        } else if (message instanceof Response) {
            handleResponse(channel, (Response) message);
        } else if (message instanceof String) {
            if (isClientSide(channel)) {
                Exception e = new Exception("Dubbo client can not supported string message: " + message + " in channel: " + channel + ", url: " + channel.getUrl());
                logger.error(e.getMessage(), e);
            } else {
                String echo = handler.telnet(channel, (String) message);
                if (echo != null && echo.length() > 0) {
                    channel.send(echo);
                }
            }
        } else {
            handler.received(exchangeChannel, message);
        }
    }

    @Override
    public void caught(Channel channel, Throwable exception) throws RemotingException {
        if (exception instanceof ExecutionException) {
            ExecutionException e = (ExecutionException) exception;
            Object msg = e.getRequest();
            if (msg instanceof Request) {
                Request req = (Request) msg;
                if (req.isTwoWay() && !req.isHeartbeat()) {
                    Response res = new Response(req.getId(), req.getVersion());
                    res.setStatus(Response.SERVER_ERROR);
                    res.setErrorMessage(StringUtils.toString(e));
                    channel.send(res);
                    return;
                }
            }
        }
        ExchangeChannel exchangeChannel = HeaderExchangeChannel.getOrAddChannel(channel);
        try {
            handler.caught(exchangeChannel, exception);
        } finally {
            HeaderExchangeChannel.removeChannelIfDisconnected(channel);
        }
    }

    @Override
    public ChannelHandler getHandler() {
        if (handler instanceof ChannelHandlerDelegate) {
            return ((ChannelHandlerDelegate) handler).getHandler();
        } else {
            return handler;
        }
    }
}
