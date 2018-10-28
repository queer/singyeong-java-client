package gg.amy.singyeong;

import io.vertx.core.Future;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketFrame;
import io.vertx.core.json.JsonObject;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import me.escoffier.vertx.completablefuture.VertxCompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author amy
 * @since 10/23/18.
 */
@SuppressWarnings("WeakerAccess")
@RequiredArgsConstructor
@Accessors(fluent = true)
public final class SingyeongSocket {
    private final SingyeongClient singyeong;
    private final AtomicReference<WebSocket> socketRef = new AtomicReference<>(null);
    private final Logger logger = LoggerFactory.getLogger(getClass());
    @Getter(AccessLevel.PACKAGE)
    private HttpClient client;
    private long heartbeatTimer = -1L;
    
    @Nonnull
    CompletableFuture<Void> connect() {
        final Future<Void> future = Future.future();
        
        client = singyeong.vertx().createHttpClient();
        doConnect(future);
        
        return VertxCompletableFuture.from(singyeong.vertx(), future);
    }
    
    private void doConnect(final Future<Void> future) {
        logger.info("Starting Singyeong connect...");
        client.websocketAbs(singyeong.serverUrl(), null, null, null,
                socket -> {
                    handleSocketConnect(socket);
                    future.complete(null);
                },
                future::fail);
    }
    
    private void handleSocketConnect(@Nonnull final WebSocket socket) {
        socket.frameHandler(this::handleFrame);
        socket.closeHandler(this::handleClose);
        socketRef.set(socket);
        logger.info("Connected to Singyeong!");
    }
    
    @SuppressWarnings("unused")
    private void handleClose(final Void __) {
        logger.warn("Disconnected from Singyeong!");
        if(heartbeatTimer != -1) {
            singyeong.vertx().cancelTimer(heartbeatTimer);
        }
        doConnect(Future.future());
    }
    
    private void handleFrame(@Nonnull final WebSocketFrame frame) {
        if(frame.isText()) {
            final JsonObject payload = new JsonObject(frame.textData());
            final SingyeongMessage msg = SingyeongMessage.fromJson(payload);
            switch(msg.op()) {
                case HELLO: {
                    final Integer heartbeatInterval = msg.data().getInteger("heartbeat_interval");
                    // IDENTIFY to allow doing everything
                    send(identify());
                    startHeartbeat(heartbeatInterval);
                    break;
                }
                case READY: {
                    // Welcome to singyeong!
                    logger.info("Welcome to singyeong!");
                    break;
                }
                case INVALID: {
                    final String error = msg.data().getString("error");
                    singyeong.vertx().eventBus().publish(SingyeongClient.SINGYEONG_INVALID_EVENT_CHANNEL,
                            new Invalid(error, msg.data()
                                    // lol
                                    .getJsonObject("d", new JsonObject().put("nonce", (String) null))
                                    .getString("nonce")));
                    break;
                }
                case DISPATCH: {
                    final JsonObject d = msg.data();
                    singyeong.vertx().eventBus().publish(SingyeongClient.SINGYEONG_DISPATCH_EVENT_CHANNEL,
                            new Dispatch(msg.timestamp(), d.getString("sender"), d.getString("nonce"),
                                    d.getJsonObject("payload")));
                    break;
                }
                case HEARTBEAT_ACK: {
                    // Avoid disconnection for another day~
                    break;
                }
                default: {
                    logger.warn("Got unknown singyeong opcode " + msg.op());
                    break;
                }
            }
        }
    }
    
    void send(@Nonnull final SingyeongMessage msg) {
        if(socketRef.get() != null) {
            socketRef.get().writeTextMessage(msg.toJson().encode());
            logger.debug("Sending Singyeong payload:\n{}", msg.toJson().encodePrettily());
        }
    }
    
    private void startHeartbeat(@Nonnegative final int heartbeatInterval) {
        // Delay a second before starting just to be safe wrt IDENTIFY
        heartbeatTimer = singyeong.vertx().setTimer(1_000L, __ ->
                singyeong.vertx().setPeriodic(heartbeatInterval, ___ ->
                        send(heartbeat())));
    }
    
    private SingyeongMessage identify() {
        return new SingyeongMessage(SingyeongOp.IDENTIFY, null, System.currentTimeMillis(),
                new JsonObject()
                        .put("client_id", singyeong.id().toString())
                        .put("application_id", singyeong.appId())
        );
    }
    
    private SingyeongMessage heartbeat() {
        return new SingyeongMessage(SingyeongOp.HEARTBEAT, null, System.currentTimeMillis(),
                new JsonObject()
                        .put("client_id", singyeong.id().toString())
        );
    }
}
