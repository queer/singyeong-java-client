package gg.amy.singyeong;

import gg.amy.vertx.SafeVertxCompletableFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * @author amy
 * @since 10/23/18.
 */
@SuppressWarnings("unused")
@Accessors(fluent = true)
public final class SingyeongClient {
    @SuppressWarnings("WeakerAccess")
    public static final String SINGYEONG_DISPATCH_EVENT_CHANNEL = "singyeong:event:dispatch";
    @SuppressWarnings("WeakerAccess")
    public static final String SINGYEONG_INVALID_EVENT_CHANNEL = "singyeong:event:invalid";
    @Getter
    private final Vertx vertx;
    @Getter
    private final WebClient client;
    @Getter
    private final String serverUrl;
    @Getter
    private final String gatewayUrl;
    @Getter
    private final String appId;
    @Getter
    private final String authentication;
    @Getter
    private final String ip;
    @Getter(AccessLevel.PACKAGE)
    private final Map<String, JsonObject> metadataCache = new ConcurrentHashMap<>();
    @Getter
    private final UUID id = UUID.randomUUID();
    @Getter
    private final List<String> tags;
    @Getter
    private SingyeongSocket socket;
    
    private SingyeongClient(@Nonnull final Vertx vertx, @Nonnull final String dsn) {
        this(vertx, dsn, Collections.emptyList());
    }
    
    private SingyeongClient(@Nonnull final Vertx vertx, @Nonnull final String dsn, @Nonnull final List<String> tags) {
        this(vertx, dsn, null, tags);
    }
    
    private SingyeongClient(@Nonnull final Vertx vertx, @Nonnull final String dsn, @Nullable final String ip) {
        this(vertx, dsn, ip, Collections.emptyList());
    }
    
    private SingyeongClient(@Nonnull final Vertx vertx, @Nonnull final String dsn, @Nullable final String ip,
                            @Nonnull final List<String> tags) {
        this.vertx = vertx;
        // TODO: Allow configuring?
        client = WebClient.create(vertx);
        this.ip = ip;
        try {
            final var uri = new URI(dsn);
            String server = "";
            final var scheme = uri.getScheme();
            if(scheme.equalsIgnoreCase("singyeong")) {
                server += "ws://";
            } else if(scheme.equalsIgnoreCase("ssingyeong")) {
                server += "wss://";
            } else {
                throw new IllegalArgumentException(scheme + " is not a valid singyeong URI scheme (expected 'singyeong' or 'ssingyeong')");
            }
            server += uri.getHost();
            if(uri.getPort() > -1) {
                server += ":" + uri.getPort();
            }
            serverUrl = server.replaceFirst("ws", "http");
            gatewayUrl = server + "/gateway/websocket";
            final String userInfo = uri.getUserInfo();
            if(userInfo == null) {
                throw new IllegalArgumentException("Didn't pass auth to singyeong DSN!");
            }
            final var split = userInfo.split(":", 2);
            appId = split[0];
            authentication = split.length != 2 ? null : split[1];
            this.tags = Collections.unmodifiableList(tags);
            
            vertx.eventBus().registerDefaultCodec(Dispatch.class, new FakeCodec<>());
            vertx.eventBus().registerDefaultCodec(Invalid.class, new FakeCodec<>());
        } catch(final URISyntaxException e) {
            throw new IllegalArgumentException("Invalid singyeong URI!", e);
        }
    }
    
    public static SingyeongClient create(@Nonnull final String dsn) {
        return create(Vertx.vertx(), dsn);
    }
    
    @SuppressWarnings("WeakerAccess")
    public static SingyeongClient create(@Nonnull final Vertx vertx, @Nonnull final String dsn) {
        return new SingyeongClient(vertx, dsn);
    }
    
    public static SingyeongClient create(@Nonnull final String dsn, @Nonnull final List<String> tags) {
        return new SingyeongClient(Vertx.vertx(), dsn, tags);
    }
    
    public static SingyeongClient create(@Nonnull final Vertx vertx, @Nonnull final String dsn,
                                         @Nonnull final List<String> tags) {
        return new SingyeongClient(vertx, dsn, tags);
    }
    
    public static SingyeongClient create(@Nonnull final Vertx vertx, @Nonnull final String dsn, @Nullable final String ip) {
        return new SingyeongClient(vertx, dsn, ip);
    }
    
    public static SingyeongClient create(@Nonnull final Vertx vertx, @Nonnull final String dsn, @Nullable final String ip,
                                         @Nonnull final List<String> tags) {
        return new SingyeongClient(vertx, dsn, ip, tags);
    }
    
    @Nonnull
    public CompletableFuture<Void> connect() {
        final var future = Future.<Void>future();
        socket = new SingyeongSocket(this);
        socket.connect()
                .thenAccept(__ -> future.complete(null))
                .exceptionally(throwable -> {
                    future.fail(throwable);
                    return null;
                });
        
        return SafeVertxCompletableFuture.from(vertx, future);
    }
    
    /**
     * Proxies an HTTP request to the target returned by the routing query.
     *
     * @param request The request to proxy.
     *
     * @return A future that completes with the response body when the request
     * is complete.
     */
    public CompletableFuture<Buffer> proxy(@Nonnull final ProxiedRequest request) {
        final var future = Future.<Buffer>future();
        final var headers = new JsonObject();
        request.headers().asMap().forEach((k, v) -> headers.put(k, new JsonArray(new ArrayList<>(v))));
        
        final var payload = new JsonObject()
                .put("method", request.method().name().toUpperCase())
                .put("route", request.route())
                .put("headers", headers)
                .put("body", request.body())
                .put("query", new JsonObject()
                        // TODO: Allow changing this
                        .put("optional", false)
                        .put("application", request.target() != null ? request.target() : request.targetTags())
                        .put("ops", request.query())
                );
        
        client.postAbs(serverUrl + "/api/v1/proxy").putHeader("Authorization", authentication)
                .sendJson(payload, ar -> {
                    if(ar.succeeded()) {
                        final var result = ar.result();
                        future.complete(result.body());
                    } else {
                        future.fail(ar.cause());
                    }
                });
        
        return SafeVertxCompletableFuture.from(vertx, future);
    }
    
    /**
     * Handle events dispatched from the server.
     *
     * @return The consumer, in case you want to unregister it.
     */
    public MessageConsumer<Dispatch> onEvent(@Nonnull final Consumer<Dispatch> consumer) {
        return vertx.eventBus().consumer(SINGYEONG_DISPATCH_EVENT_CHANNEL, m -> consumer.accept(m.body()));
    }
    
    /**
     * Handle messages from the server telling you that you sent a bad message.
     *
     * @return The consumer, in case you want to unregister it.
     */
    @SuppressWarnings("WeakerAccess")
    public MessageConsumer<Invalid> onInvalid(@Nonnull final Consumer<Invalid> consumer) {
        return vertx.eventBus().consumer(SINGYEONG_INVALID_EVENT_CHANNEL, m -> consumer.accept(m.body()));
    }
    
    /**
     * Send a message to a single target node matching the routing query
     * provided. If no nodes match, an {@link SingyeongOp#INVALID} event will
     * be fired. See {@link #onInvalid(Consumer)}.
     *
     * @param appId   The application id to target.
     * @param query   The routing query. See {@link QueryBuilder}.
     * @param payload The payload to send. Will be converted to JSON.
     * @param <T>     Type of the payload.
     */
    public <T> void send(@Nonnull final String appId, @Nonnull final JsonArray query, @Nullable final T payload) {
        send(appId, null, query, payload);
    }
    
    /**
     * Send a message to a single target node matching the routing query
     * provided. If no nodes match, an {@link SingyeongOp#INVALID} event will
     * be fired. See {@link #onInvalid(Consumer)}.
     *
     * @param appId   The application id to target.
     * @param nonce   The nonce, used for awaiting responses.
     * @param query   The routing query. See {@link QueryBuilder}.
     * @param payload The payload to send. Will be converted to JSON.
     * @param <T>     Type of the payload.
     */
    @SuppressWarnings("WeakerAccess")
    public <T> void send(@Nonnull final String appId, @Nullable final String nonce, @Nonnull final JsonArray query,
                         @Nullable final T payload) {
        send(appId, nonce, query, payload, false);
    }
    
    /**
     * Send a message to a single target node matching the routing query
     * provided. If no nodes match, an {@link SingyeongOp#INVALID} event will
     * be fired. See {@link #onInvalid(Consumer)}.
     *
     * @param appId    The application id to target.
     * @param nonce    The nonce, used for awaiting responses.
     * @param query    The routing query. See {@link QueryBuilder}.
     * @param payload  The payload to send. Will be converted to JSON.
     * @param optional Whether or not the routing query is optional.
     * @param <T>      Type of the payload.
     */
    @SuppressWarnings("WeakerAccess")
    public <T> void send(@Nonnull final String appId, @Nullable final String nonce, @Nonnull final JsonArray query,
                         @Nullable final T payload, final boolean optional) {
        final var msg = createDispatch("SEND", appId, nonce, query, optional, payload);
        socket.send(msg);
    }
    
    /**
     * Send a message to a all target nodes matching the routing query
     * provided. If no nodes match, an {@link SingyeongOp#INVALID} event will
     * be fired. See {@link #onInvalid(Consumer)}.
     *
     * @param appId   The application id to target.
     * @param query   The routing query. See {@link QueryBuilder}.
     * @param payload The payload to send. Will be converted to JSON.
     * @param <T>     Type of the payload.
     */
    public <T> void broadcast(@Nonnull final String appId, @Nonnull final JsonArray query, @Nullable final T payload) {
        broadcast(appId, null, query, payload);
    }
    
    /**
     * Send a message to a all target nodes matching the routing query
     * provided. If no nodes match, an {@link SingyeongOp#INVALID} event will
     * be fired. See {@link #onInvalid(Consumer)}.
     *
     * @param appId   The application id to target.
     * @param nonce   The nonce, used for awaiting responses.
     * @param query   The routing query. See {@link QueryBuilder}.
     * @param payload The payload to send. Will be converted to JSON.
     * @param <T>     Type of the payload.
     */
    @SuppressWarnings("WeakerAccess")
    public <T> void broadcast(@Nonnull final String appId, @Nullable final String nonce, @Nonnull final JsonArray query,
                              @Nullable final T payload) {
        broadcast(appId, nonce, query, payload, false);
    }
    
    /**
     * Send a message to a all target nodes matching the routing query
     * provided. If no nodes match, an {@link SingyeongOp#INVALID} event will
     * be fired. See {@link #onInvalid(Consumer)}.
     *
     * @param appId    The application id to target.
     * @param nonce    The nonce, used for awaiting responses.
     * @param query    The routing query. See {@link QueryBuilder}.
     * @param payload  The payload to send. Will be converted to JSON.
     * @param optional Whether or not the routing query is optional.
     * @param <T>      Type of the payload.
     */
    @SuppressWarnings("WeakerAccess")
    public <T> void broadcast(@Nonnull final String appId, @Nullable final String nonce, @Nonnull final JsonArray query,
                              @Nullable final T payload, final boolean optional) {
        final var msg = createDispatch("BROADCAST", appId, nonce, query, optional, payload);
        socket.send(msg);
    }
    
    /**
     * Send a message to a single target node matching the routing query
     * provided. If no nodes match, an {@link SingyeongOp#INVALID} event will
     * be fired. See {@link #onInvalid(Consumer)}.
     *
     * @param tags    The application tags to use for discovering a target.
     * @param query   The routing query. See {@link QueryBuilder}.
     * @param payload The payload to send. Will be converted to JSON.
     * @param <T>     Type of the payload.
     */
    public <T> void send(@Nonnull final List<String> tags, @Nonnull final JsonArray query, @Nullable final T payload) {
        send(tags, null, query, payload);
    }
    
    /**
     * Send a message to a single target node matching the routing query
     * provided. If no nodes match, an {@link SingyeongOp#INVALID} event will
     * be fired. See {@link #onInvalid(Consumer)}.
     *
     * @param tags    The application tags to use for discovering a target.
     * @param nonce   The nonce, used for awaiting responses.
     * @param query   The routing query. See {@link QueryBuilder}.
     * @param payload The payload to send. Will be converted to JSON.
     * @param <T>     Type of the payload.
     */
    @SuppressWarnings("WeakerAccess")
    public <T> void send(@Nonnull final List<String> tags, @Nullable final String nonce, @Nonnull final JsonArray query,
                         @Nullable final T payload) {
        send(tags, nonce, query, payload, false);
    }
    
    /**
     * Send a message to a single target node matching the routing query
     * provided. If no nodes match, an {@link SingyeongOp#INVALID} event will
     * be fired. See {@link #onInvalid(Consumer)}.
     *
     * @param tags     The application tags to use for discovering a target.
     * @param nonce    The nonce, used for awaiting responses.
     * @param query    The routing query. See {@link QueryBuilder}.
     * @param payload  The payload to send. Will be converted to JSON.
     * @param optional Whether or not the routing query is optional.
     * @param <T>      Type of the payload.
     */
    @SuppressWarnings("WeakerAccess")
    public <T> void send(@Nonnull final List<String> tags, @Nullable final String nonce, @Nonnull final JsonArray query,
                         @Nullable final T payload, final boolean optional) {
        final var msg = createDispatch("SEND", tags, nonce, query, optional, payload);
        socket.send(msg);
    }
    
    /**
     * Send a message to a all target nodes matching the routing query
     * provided. If no nodes match, an {@link SingyeongOp#INVALID} event will
     * be fired. See {@link #onInvalid(Consumer)}.
     *
     * @param tags    The application tags to use for discovering a target.
     * @param query   The routing query. See {@link QueryBuilder}.
     * @param payload The payload to send. Will be converted to JSON.
     * @param <T>     Type of the payload.
     */
    public <T> void broadcast(@Nonnull final List<String> tags, @Nonnull final JsonArray query, @Nullable final T payload) {
        broadcast(tags, null, query, payload);
    }
    
    /**
     * Send a message to a all target nodes matching the routing query
     * provided. If no nodes match, an {@link SingyeongOp#INVALID} event will
     * be fired. See {@link #onInvalid(Consumer)}.
     *
     * @param tags    The application tags to use for discovering a target.
     * @param nonce   The nonce, used for awaiting responses.
     * @param query   The routing query. See {@link QueryBuilder}.
     * @param payload The payload to send. Will be converted to JSON.
     * @param <T>     Type of the payload.
     */
    @SuppressWarnings("WeakerAccess")
    public <T> void broadcast(@Nonnull final List<String> tags, @Nullable final String nonce, @Nonnull final JsonArray query,
                              @Nullable final T payload) {
        broadcast(tags, nonce, query, payload, false);
    }
    
    /**
     * Send a message to a all target nodes matching the routing query
     * provided. If no nodes match, an {@link SingyeongOp#INVALID} event will
     * be fired. See {@link #onInvalid(Consumer)}.
     *
     * @param tags     The application tags to use for discovering a target.
     * @param nonce    The nonce, used for awaiting responses.
     * @param query    The routing query. See {@link QueryBuilder}.
     * @param payload  The payload to send. Will be converted to JSON.
     * @param optional Whether or not the routing query is optional.
     * @param <T>      Type of the payload.
     */
    @SuppressWarnings("WeakerAccess")
    public <T> void broadcast(@Nonnull final List<String> tags, @Nullable final String nonce, @Nonnull final JsonArray query,
                              @Nullable final T payload, final boolean optional) {
        final var msg = createDispatch("BROADCAST", tags, nonce, query, optional, payload);
        socket.send(msg);
    }
    
    private <T> SingyeongMessage createDispatch(@Nonnull final String type, @Nonnull final Object application,
                                                @Nullable final String nonce, @Nonnull final JsonArray query,
                                                final boolean optional, @Nullable final T payload) {
        return new SingyeongMessage(SingyeongOp.DISPATCH, type, System.currentTimeMillis(),
                new JsonObject()
                        .put("sender", id.toString())
                        .put("target", new JsonObject()
                                .put("optional", optional)
                                .put("application", application)
                                .put("ops", query)
                        )
                        .put("nonce", nonce)
                        .put("payload", JsonObject.mapFrom(payload))
        );
    }
    
    /**
     * Update this client's metadata on the server.
     *
     * @param key  The metadata key to set.
     * @param type The type of the metadata. Will be validated by the server.
     * @param data The value to set for the metadata key.
     * @param <T>  The Java type of the metadata.
     */
    public <T> void updateMetadata(@Nonnull final String key, @Nonnull final SingyeongType type, @Nonnull final T data) {
        final var metadataValue = new JsonObject().put("type", type.name().toLowerCase()).put("value", data);
        metadataCache.put(key, metadataValue);
        final var msg = new SingyeongMessage(SingyeongOp.DISPATCH, "UPDATE_METADATA",
                System.currentTimeMillis(),
                new JsonObject().put(key, metadataValue)
        );
        socket.send(msg);
    }
    
    private <T> void codec(@Nonnull final Class<T> cls) {
        vertx.eventBus().registerDefaultCodec(cls, new JsonPojoCodec<>(cls));
    }
    
    private static class FakeCodec<T> implements MessageCodec<T, Object> {
        @Override
        public void encodeToWire(final Buffer buffer, final T dispatch) {
            
        }
        
        @Override
        public Object decodeFromWire(final int pos, final Buffer buffer) {
            return null;
        }
        
        @Override
        public Object transform(final T dispatch) {
            return dispatch;
        }
        
        @Override
        public String name() {
            return "noop" + new Random().nextInt();
        }
        
        @Override
        public byte systemCodecID() {
            return -1;
        }
    }
}
