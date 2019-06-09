package gg.amy.singyeong.data;

import io.vertx.core.json.JsonObject;
import lombok.Value;
import lombok.experimental.Accessors;

/**
 * @author amy
 * @since 10/23/18.
 */
@Value
@Accessors(fluent = true)
public class Dispatch {
    private long timestamp;
    private String sender;
    private String nonce;
    private JsonObject data;
}
