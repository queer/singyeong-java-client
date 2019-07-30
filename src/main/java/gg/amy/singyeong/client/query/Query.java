package gg.amy.singyeong.client.query;

import io.vertx.core.json.JsonArray;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.util.Collection;

/**
 * @author amy
 * @since 6/9/19.
 */
@Getter
@Accessors(fluent = true)
@AllArgsConstructor(access = AccessLevel.PACKAGE)
public final class Query {
    private final String target;
    private final Collection<String> tags;
    private final JsonArray ops;
    private final boolean optional;
    private final boolean restricted;
    private final boolean consistent;
    private final String hashKey;
}
