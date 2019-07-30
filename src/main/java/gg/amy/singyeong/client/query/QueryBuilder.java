package gg.amy.singyeong.client.query;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author amy
 * @since 10/23/18.
 */
@SuppressWarnings("unused")
public final class QueryBuilder {
    private final Collection<JsonObject> ops = new ArrayList<>();
    private boolean optional;
    private String hashKey;
    private String target;
    private Collection<String> tags;
    
    public <T> QueryBuilder eq(@Nonnull final String key, @Nullable final T value) {
        ops.add(new JsonObject().put(key, new JsonObject().put("$eq", value)));
        return this;
    }
    
    public <T> QueryBuilder ne(@Nonnull final String key, @Nullable final T value) {
        ops.add(new JsonObject().put(key, new JsonObject().put("$ne", value)));
        return this;
    }
    
    public <T> QueryBuilder gt(@Nonnull final String key, @Nullable final T value) {
        ops.add(new JsonObject().put(key, new JsonObject().put("$gt", value)));
        return this;
    }
    
    public <T> QueryBuilder gte(@Nonnull final String key, @Nullable final T value) {
        ops.add(new JsonObject().put(key, new JsonObject().put("$gte", value)));
        return this;
    }
    
    public <T> QueryBuilder lt(@Nonnull final String key, @Nullable final T value) {
        ops.add(new JsonObject().put(key, new JsonObject().put("$lt", value)));
        return this;
    }
    
    public <T> QueryBuilder lte(@Nonnull final String key, @Nullable final T value) {
        ops.add(new JsonObject().put(key, new JsonObject().put("$lte", value)));
        return this;
    }
    
    public <T> QueryBuilder in(@Nonnull final String key, @Nullable final T value) {
        ops.add(new JsonObject().put(key, new JsonObject().put("$in", value)));
        return this;
    }
    
    public <T> QueryBuilder nin(@Nonnull final String key, @Nullable final T value) {
        ops.add(new JsonObject().put(key, new JsonObject().put("$nin", value)));
        return this;
    }
    
    public <T> QueryBuilder contains(@Nonnull final String key, @Nullable final T value) {
        ops.add(new JsonObject().put(key, new JsonObject().put("$contains", value)));
        return this;
    }
    
    public <T> QueryBuilder ncontains(@Nonnull final String key, @Nullable final T value) {
        ops.add(new JsonObject().put(key, new JsonObject().put("$ncontains", value)));
        return this;
    }
    
    public QueryBuilder and(@Nonnull final String key, @Nonnull final QueryBuilder value) {
        if(value.ops.isEmpty()) {
            throw new IllegalArgumentException("Passed QueryBuilder doesn't have any ops!");
        }
        ops.add(new JsonObject().put(key, new JsonObject().put("$and", value.ops)));
        return this;
    }
    
    public QueryBuilder or(@Nonnull final String key, @Nonnull final QueryBuilder value) {
        if(value.ops.isEmpty()) {
            throw new IllegalArgumentException("Passed QueryBuilder doesn't have any ops!");
        }
        ops.add(new JsonObject().put(key, new JsonObject().put("$or", value.ops)));
        return this;
    }
    
    public QueryBuilder nor(@Nonnull final String key, @Nonnull final QueryBuilder value) {
        if(value.ops.isEmpty()) {
            throw new IllegalArgumentException("Passed QueryBuilder doesn't have any ops!");
        }
        ops.add(new JsonObject().put(key, new JsonObject().put("$nor", value.ops)));
        return this;
    }
    
    /**
     * Directly adds the specified ops to the list of ops. <strong>Input is not
     * validated.</strong>
     *
     * @param ops The ops to add.
     *
     * @return Itself.
     */
    public QueryBuilder withOps(@Nonnull final Collection<JsonObject> ops) {
        this.ops.addAll(ops);
        return this;
    }
    
    public QueryBuilder optional(final boolean optional) {
        this.optional = optional;
        return this;
    }
    
    public QueryBuilder target(final String target) {
        if(tags != null && !tags.isEmpty()) {
            throw new IllegalStateException("Attempted to set target string when target tags are already set!");
        }
        this.target = target;
        return this;
    }
    
    public QueryBuilder target(final Collection<String> tags) {
        if(target != null && !target.isEmpty()) {
            throw new IllegalStateException("Attempted to set target tags when target string is already set!");
        }
        this.tags = tags;
        return this;
    }
    
    public QueryBuilder hashKey(final String hashKey) {
        this.hashKey = hashKey;
        return this;
    }
    
    public Query build() {
        return new Query(target, tags, new JsonArray(List.copyOf(ops)), optional, hashKey != null, hashKey);
    }
}
