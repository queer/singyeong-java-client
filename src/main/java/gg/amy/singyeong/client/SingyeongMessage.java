package gg.amy.singyeong.client;

import io.vertx.core.json.JsonObject;
import lombok.Value;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;

/**
 * @author amy
 * @since 10/23/18.
 */
@Value
@Accessors(fluent = true)
public class SingyeongMessage {
    private SingyeongOp op;
    private String type;
    private long timestamp;
    private JsonObject data;
    
    static SingyeongMessage fromJson(@Nonnull final JsonObject json) {
        JsonObject data;
        final var d = json.getValue("d");
        if(d instanceof JsonObject) {
            data = (JsonObject) d;
        } else if(d instanceof String) {
            data = new JsonObject((String) d);
        } else {
            throw new IllegalStateException("Couldn't parse :d as json object (really wtf am I doing here...)");
        }
        return new SingyeongMessage(SingyeongOp.fromOp(json.getInteger("op")),
                json.getString("t", null), json.getLong("ts"),
                data);
    }
    
    JsonObject toJson() {
        return new JsonObject()
                .put("op", op.code())
                .put("t", type)
                .put("ts", timestamp)
                .put("d", data)
                ;
    }
}
