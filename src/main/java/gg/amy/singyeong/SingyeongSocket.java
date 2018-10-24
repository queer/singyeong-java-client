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

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author amy
 * @since 10/23/18.
 */
@RequiredArgsConstructor
@Accessors(fluent = true)
public final class SingyeongSocket {
    private final SingyeongClient singyeong;
    @Getter(AccessLevel.PACKAGE)
    private HttpClient client;
    private final AtomicReference<WebSocket> socketRef = new AtomicReference<>(null);
    
    @Nonnull
    CompletableFuture<Void> connect() {
        final Future<Void> future = Future.future();
        
        client = singyeong.vertx().createHttpClient();
        client.websocketAbs(singyeong.serverUrl(), null, null, null,
                socket -> {
                    future.complete(null);
                    handleSocketConnect(socket);
                },
                future::fail);
        
        return VertxCompletableFuture.from(singyeong.vertx(), future);
    }
    
    private void handleSocketConnect(@Nonnull final WebSocket socket) {
        socket.frameHandler(this::handleFrame);
    }
    
    private void handleFrame(@Nonnull final WebSocketFrame frame) {
        if(frame.isText()) {
            final JsonObject payload = new JsonObject(frame.textData());
            final SingyeongMessage msg = SingyeongMessage.fromJson(payload);
            switch(msg.op()) {
                case HELLO: {
                    final Integer heartbeatInterval = msg.data().getInteger("heartbeat_interval");
                    startHeartbeat(heartbeatInterval);
                    // IDENTIFY to allow doing everything
                    send(identify());
                    break;
                }
                case READY: {
                    // Welcome to singyeong!
                    break;
                }
                case INVALID: {
                    final String error = msg.data().getString("error");
                    singyeong.vertx().eventBus().publish(SingyeongClient.SINGYEONG_INVALID_EVENT_CHANNEL,
                            new Invalid(error));
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
            }
        }
    }
    
    void send(@Nonnull final SingyeongMessage msg) {
        if(socketRef.get() != null) {
            socketRef.get().writeTextMessage(msg.toJson().encode());
        }
    }
    
    private void startHeartbeat(@Nonnegative final int heartbeatInterval) {
        // Delay a second before starting just to be safe wrt IDENTIFY
        singyeong.vertx().setTimer(1_000L, __ ->
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
        return new SingyeongMessage(SingyeongOp.IDENTIFY, null, System.currentTimeMillis(),
                new JsonObject()
                        .put("client_id", singyeong.id().toString())
                        .put("application_id", singyeong.appId())
        );
    }
}
