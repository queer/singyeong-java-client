package gg.amy.singyeong;

import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Value;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * A proxied request that Singyeong can execute on behalf of the client.
 *
 * @author amy
 * @since 6/5/19.
 */
@Value
@Accessors(fluent = true)
@Builder(toBuilder = true)
@SuppressWarnings("WeakerAccess")
public final class ProxiedRequest {
    private final HttpMethod method;
    private final String route;
    private final String target;
    private final List<String> targetTags;
    private final JsonArray query;
    @Default
    private final Multimap<String, String> headers = Multimaps.newMultimap(new HashMap<>(), ArrayList::new);
    private final Object body;
}
