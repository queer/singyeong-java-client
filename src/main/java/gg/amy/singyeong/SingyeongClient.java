package gg.amy.singyeong;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.Getter;
import lombok.experimental.Accessors;
import me.escoffier.vertx.completablefuture.VertxCompletableFuture;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * @author amy
 * @since 10/23/18.
 */
@Accessors(fluent = true)
public class SingyeongClient {
    public static final String SINGYEONG_DISPATCH_EVENT_CHANNEL = "singyeong:event:dispatch";
    public static final String SINGYEONG_INVALID_EVENT_CHANNEL = "singyeong:event:invalid";
    @Getter
    private final Vertx vertx;
    @Getter
    private final String serverUrl;
    @Getter
    private final String appId;
    @Getter
    private final UUID id = UUID.randomUUID();
    @Getter
    private SingyeongSocket socket;
    
    public SingyeongClient(@Nonnull final String serverUrl, @Nonnull final String appId) {
        this(serverUrl, Vertx.vertx(), appId);
    }
    
    @SuppressWarnings("WeakerAccess")
    public SingyeongClient(@Nonnull final String serverUrl, @Nonnull final Vertx vertx, @Nonnull final String appId) {
        this.serverUrl = serverUrl;
        this.vertx = vertx;
        this.appId = appId;
        
        codec(Dispatch.class);
        codec(Invalid.class);
    }
    
    @Nonnull
    public CompletableFuture<Void> connect() {
        final Future<Void> future = Future.future();
        socket = new SingyeongSocket(this);
        socket.connect()
                .thenAccept(__ -> future.complete(null))
                .exceptionally(throwable -> {
                    future.fail(throwable);
                    return null;
                });
        
        return VertxCompletableFuture.from(vertx, future);
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
        final var msg = createDispatch("SEND", appId, null, query, payload);
        socket.send(msg);
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
    public <T> void send(@Nonnull final String appId, final String nonce, @Nonnull final JsonArray query, @Nullable final T payload) {
        final var msg = createDispatch("SEND", appId, nonce, query, payload);
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
        final var msg = createDispatch("BROADCAST", appId, null, query, payload);
        socket.send(msg);
    }
    
    /**
     * Send a message to a all target nodes matching the routing query
     * provided. If no nodes match, an {@link SingyeongOp#INVALID} event will
     * be fired. See {@link #onInvalid(Consumer)}.
     *
     * @param appId   The application id to target.
     * @param nonce   THe nonce, used for awaiting responses.
     * @param query   The routing query. See {@link QueryBuilder}.
     * @param payload The payload to send. Will be converted to JSON.
     * @param <T>     Type of the payload.
     */
    public <T> void broadcast(@Nonnull final String appId, @Nonnull final String nonce, @Nonnull final JsonArray query, @Nullable final T payload) {
        final var msg = createDispatch("BROADCAST", appId, nonce, query, payload);
        socket.send(msg);
    }
    
    public <T> void updateMetadata(@Nonnull final String key, @Nonnull final SingyeongType type, @Nonnull final T data) {
        final var msg = new SingyeongMessage(SingyeongOp.DISPATCH, "UPDATE_METADATA",
                System.currentTimeMillis(),
                new JsonObject().put(key, new JsonObject().put("type", type.name().toLowerCase()).put("value", data))
        );
        socket.send(msg);
    }
    
    private <T> SingyeongMessage createDispatch(@Nonnull final String type, @Nonnull final String appId,
                                                @Nullable final String nonce, @Nonnull final JsonArray query,
                                                @Nullable final T payload) {
        return new SingyeongMessage(SingyeongOp.DISPATCH, type, System.currentTimeMillis(),
                new JsonObject()
                        .put("sender", id.toString())
                        .put("target", new JsonObject()
                                .put("application", appId)
                                .put("ops", query)
                        )
                        .put("nonce", nonce)
                        .put("payload", JsonObject.mapFrom(payload))
        );
    }
    
    private <T> void codec(@Nonnull final Class<T> cls) {
        vertx.eventBus().registerDefaultCodec(cls, new JsonPojoCodec<>(cls));
    }
}
