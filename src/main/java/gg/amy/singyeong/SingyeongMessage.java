package gg.amy.singyeong;

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
    
    public static SingyeongMessage fromJson(@Nonnull final JsonObject json) {
        return new SingyeongMessage(SingyeongOp.fromOp(json.getInteger("op")),
                json.getString("t", null), json.getLong("ts"),
                json.getJsonObject("d"));
    }
    
    public JsonObject toJson() {
        return new JsonObject()
                .put("op", op.code())
                .put("t", type)
                .put("ts", timestamp)
                .put("d", data)
                ;
    }
}
