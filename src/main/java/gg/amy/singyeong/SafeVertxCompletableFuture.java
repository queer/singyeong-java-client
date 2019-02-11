/*
 * Copyright (c) 2019 amy, All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  1. Redistributions of source code must retain the above copyright notice, this
 *     list of conditions and the following disclaimer.
 *  2. Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *  3. Neither the name of the copyright holder nor the names of its contributors
 *     may be used to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 *  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 *  DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 *  FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 *  DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 *  SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 *  CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 *  OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package gg.amy.singyeong;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

@SuppressWarnings({"WeakerAccess", "unused"})
public class SafeVertxCompletableFuture<T> extends CompletableFuture<T> {
    private final Executor executor;
    private final Vertx vertx;
    private final Context context;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    public SafeVertxCompletableFuture(final Vertx vertx, final Context context) {
        this.vertx = vertx;
        this.context = context;
        executor = r -> context.runOnContext(__ -> r.run());
    }
    
    public SafeVertxCompletableFuture(final Vertx vertx) {
        this(vertx, vertx.getOrCreateContext());
    }
    
    private SafeVertxCompletableFuture(final Vertx vertx, final Context context, final CompletionStage<T> future) {
        this(vertx, context);
        future.whenComplete((res, err) -> {
            if(err != null) {
                completeExceptionally(err);
            } else {
                complete(res);
            }
        });
    }
    
    public static <T> SafeVertxCompletableFuture<T> from(final Vertx vertx, final CompletionStage<T> future) {
        return from(vertx, vertx.getOrCreateContext(), future);
    }
    
    public static <T> SafeVertxCompletableFuture<T> from(final Vertx vertx, final Future<T> future) {
        return from(vertx, vertx.getOrCreateContext(), future);
    }
    
    public static <T> SafeVertxCompletableFuture<T> from(final Vertx vertx, final Context context, final CompletionStage<T> future) {
        final SafeVertxCompletableFuture<T> res = new SafeVertxCompletableFuture<>(vertx, context);
        future.whenComplete((result, error) -> {
            if(context == Vertx.currentContext()) {
                res.complete(result, error);
            } else {
                res.context.runOnContext(v -> res.complete(result, error));
            }
        });
        return res;
    }
    
    public static <T> SafeVertxCompletableFuture<T> from(final Vertx vertx, final Context context, final Future<T> future) {
        final SafeVertxCompletableFuture<T> res = new SafeVertxCompletableFuture<>(vertx, context);
        future.setHandler(ar -> {
            if(context == Vertx.currentContext()) {
                res.completeFromAsyncResult(ar);
            } else {
                res.context.runOnContext(v -> res.completeFromAsyncResult(ar));
            }
        });
        return res;
    }
    
    public static SafeVertxCompletableFuture<Void> allOf(final Vertx vertx, final CompletableFuture<?>... futures) {
        final CompletableFuture<Void> all = CompletableFuture.allOf(futures);
        return from(vertx, all);
    }
    
    public static SafeVertxCompletableFuture<Void> allOf(final Vertx vertx, final Context context, final CompletableFuture<?>... futures) {
        final CompletableFuture<Void> all = CompletableFuture.allOf(futures);
        return from(vertx, context, all);
    }
    
    public static SafeVertxCompletableFuture<Object> anyOf(final Vertx vertx, final CompletableFuture<?>... futures) {
        final CompletableFuture<Object> all = CompletableFuture.anyOf(futures);
        return from(vertx, all);
    }
    
    public static SafeVertxCompletableFuture<Object> anyOf(final Vertx vertx, final Context context, final CompletableFuture<?>... futures) {
        final CompletableFuture<Object> all = CompletableFuture.anyOf(futures);
        return from(vertx, context, all);
    }
    
    public SafeVertxCompletableFuture<T> withContext() {
        final Context context = Vertx.currentContext();
        return withContext(context);
    }
    
    public SafeVertxCompletableFuture<T> withContext(final Context context) {
        final SafeVertxCompletableFuture<T> future = new SafeVertxCompletableFuture<>(vertx, context);
        whenComplete((res, err) -> {
            if(err != null) {
                future.completeExceptionally(err);
            } else {
                future.complete(res);
            }
        });
        return future;
    }
    
    // ============= Composite Future implementation =============
    
    public Context context() {
        return context;
    }
    
    @Override
    public <U> SafeVertxCompletableFuture<U> thenApply(final Function<? super T, ? extends U> fn) {
        return new SafeVertxCompletableFuture<>(vertx, context, super.thenApply(fn));
    }
    
    @Override
    public <U> SafeVertxCompletableFuture<U> thenApplyAsync(final Function<? super T, ? extends U> fn, final Executor executor) {
        return new SafeVertxCompletableFuture<>(vertx, context, super.thenApplyAsync(fn, executor));
    }
    
    @Override
    public SafeVertxCompletableFuture<Void> thenAcceptAsync(final Consumer<? super T> action, final Executor executor) {
        return new SafeVertxCompletableFuture<>(vertx, context, super.thenAcceptAsync(action, executor));
    }
    
    @Override
    public SafeVertxCompletableFuture<Void> thenRun(final Runnable action) {
        return new SafeVertxCompletableFuture<>(vertx, context, super.thenRun(action));
    }
    
    @Override
    public SafeVertxCompletableFuture<Void> thenRunAsync(final Runnable action, final Executor executor) {
        return new SafeVertxCompletableFuture<>(vertx, context, super.thenRunAsync(action, executor));
    }
    
    @Override
    public <U, V> SafeVertxCompletableFuture<V> thenCombine(final CompletionStage<? extends U> other, final BiFunction<? super T, ? super U, ? extends V> fn) {
        return new SafeVertxCompletableFuture<>(vertx, context, super.thenCombine(other, fn));
    }
    
    @Override
    public <U> SafeVertxCompletableFuture<Void> thenAcceptBoth(final CompletionStage<? extends U> other, final BiConsumer<? super T, ? super U> action) {
        return new SafeVertxCompletableFuture<>(vertx, context, super.thenAcceptBoth(other, action));
    }
    
    @Override
    public <U> SafeVertxCompletableFuture<Void> thenAcceptBothAsync(final CompletionStage<? extends U> other, final BiConsumer<? super T, ? super U> action, final Executor executor) {
        return new SafeVertxCompletableFuture<>(vertx, context, super.thenAcceptBothAsync(other, action, executor));
    }
    
    @Override
    public SafeVertxCompletableFuture<Void> runAfterBoth(final CompletionStage<?> other, final Runnable action) {
        return new SafeVertxCompletableFuture<>(vertx, context, super.runAfterBoth(other, action));
    }
    
    @Override
    public SafeVertxCompletableFuture<Void> runAfterBothAsync(final CompletionStage<?> other, final Runnable action, final Executor executor) {
        return new SafeVertxCompletableFuture<>(vertx, context, super.runAfterBothAsync(other, action, executor));
    }
    
    @Override
    public <U> SafeVertxCompletableFuture<U> applyToEither(final CompletionStage<? extends T> other, final Function<? super T, U> fn) {
        return new SafeVertxCompletableFuture<>(vertx, context, super.applyToEither(other, fn));
    }
    
    @Override
    public <U> SafeVertxCompletableFuture<U> applyToEitherAsync(final CompletionStage<? extends T> other, final Function<? super T, U> fn, final Executor executor) {
        return new SafeVertxCompletableFuture<>(vertx, context, super.applyToEitherAsync(other, fn, executor));
    }
    
    @Override
    public SafeVertxCompletableFuture<Void> acceptEither(final CompletionStage<? extends T> other, final Consumer<? super T> action) {
        return new SafeVertxCompletableFuture<>(vertx, context, super.acceptEither(other, action));
    }
    
    @Override
    public SafeVertxCompletableFuture<Void> acceptEitherAsync(final CompletionStage<? extends T> other, final Consumer<? super T> action, final Executor executor) {
        return new SafeVertxCompletableFuture<>(vertx, context, super.acceptEitherAsync(other, action, executor));
    }
    
    @Override
    public SafeVertxCompletableFuture<Void> runAfterEither(final CompletionStage<?> other, final Runnable action) {
        return new SafeVertxCompletableFuture<>(vertx, context, super.runAfterEither(other, action));
    }
    
    @Override
    public SafeVertxCompletableFuture<Void> runAfterEitherAsync(final CompletionStage<?> other, final Runnable action, final Executor executor) {
        return new SafeVertxCompletableFuture<>(vertx, context, super.runAfterEitherAsync(other, action, executor));
    }
    
    @Override
    public <U> SafeVertxCompletableFuture<U> thenCompose(final Function<? super T, ? extends CompletionStage<U>> fn) {
        return new SafeVertxCompletableFuture<>(vertx, context, super.thenCompose(fn));
    }
    
    @Override
    public SafeVertxCompletableFuture<T> whenComplete(final BiConsumer<? super T, ? super Throwable> action) {
        return new SafeVertxCompletableFuture<>(vertx, context, super.whenComplete(action));
    }
    
    @Override
    public SafeVertxCompletableFuture<T> whenCompleteAsync(final BiConsumer<? super T, ? super Throwable> action, final Executor executor) {
        return new SafeVertxCompletableFuture<>(vertx, context, super.whenCompleteAsync(action, executor));
    }
    
    @Override
    public <U> SafeVertxCompletableFuture<U> handle(final BiFunction<? super T, Throwable, ? extends U> fn) {
        return new SafeVertxCompletableFuture<>(vertx, context, super.handle(fn));
    }
    
    @Override
    public <U> SafeVertxCompletableFuture<U> handleAsync(final BiFunction<? super T, Throwable, ? extends U> fn, final Executor executor) {
        return new SafeVertxCompletableFuture<>(vertx, context, super.handleAsync(fn, executor));
    }
    
    @Override
    public <U> SafeVertxCompletableFuture<U> thenApplyAsync(final Function<? super T, ? extends U> fn) {
        return new SafeVertxCompletableFuture<>(vertx, context, super.thenApplyAsync(fn, executor));
    }
    
    @Override
    public SafeVertxCompletableFuture<Void> thenAccept(final Consumer<? super T> action) {
        return new SafeVertxCompletableFuture<>(vertx, context, super.thenAccept(action));
    }
    
    @Override
    public SafeVertxCompletableFuture<Void> thenAcceptAsync(final Consumer<? super T> action) {
        return new SafeVertxCompletableFuture<>(vertx, context, super.thenAcceptAsync(action, executor));
    }
    
    @Override
    public SafeVertxCompletableFuture<Void> thenRunAsync(final Runnable action) {
        return new SafeVertxCompletableFuture<>(vertx, context, super.thenRunAsync(action, executor));
    }
    
    @Override
    public <U, V> SafeVertxCompletableFuture<V> thenCombineAsync(final CompletionStage<? extends U> other,
                                                                 final BiFunction<? super T, ? super U, ? extends V> fn) {
        return new SafeVertxCompletableFuture<>(vertx, context, super.thenCombineAsync(other, fn, executor));
    }
    
    @Override
    public <U> SafeVertxCompletableFuture<Void> thenAcceptBothAsync(final CompletionStage<? extends U> other,
                                                                    final BiConsumer<? super T, ? super U> action) {
        return new SafeVertxCompletableFuture<>(vertx, context, super.thenAcceptBothAsync(other, action, executor));
    }
    
    @Override
    public SafeVertxCompletableFuture<Void> runAfterBothAsync(final CompletionStage<?> other, final Runnable action) {
        return new SafeVertxCompletableFuture<>(vertx, context, super.runAfterBothAsync(other, action, executor));
    }
    
    @Override
    public <U> SafeVertxCompletableFuture<U> applyToEitherAsync(final CompletionStage<? extends T> other, final Function<? super T, U> fn) {
        return new SafeVertxCompletableFuture<>(vertx, context, super.applyToEitherAsync(other, fn, executor));
    }
    
    @Override
    public SafeVertxCompletableFuture<Void> acceptEitherAsync(final CompletionStage<? extends T> other, final Consumer<? super T> action) {
        return new SafeVertxCompletableFuture<>(vertx, context, super.acceptEitherAsync(other, action, executor));
    }
    
    @Override
    public SafeVertxCompletableFuture<Void> runAfterEitherAsync(final CompletionStage<?> other, final Runnable action) {
        return new SafeVertxCompletableFuture<>(vertx, context, super.runAfterEitherAsync(other, action, executor));
    }
    
    @Override
    public <U> SafeVertxCompletableFuture<U> thenComposeAsync(final Function<? super T, ? extends CompletionStage<U>> fn) {
        return new SafeVertxCompletableFuture<>(vertx, context, super.thenComposeAsync(fn, executor));
    }
    
    @Override
    public <U> SafeVertxCompletableFuture<U> thenComposeAsync(final Function<? super T, ? extends CompletionStage<U>> fn, final Executor executor) {
        return new SafeVertxCompletableFuture<>(vertx, context, super.thenComposeAsync(fn, executor));
    }
    
    public <U, V> SafeVertxCompletableFuture<V> thenCombineAsync(
            final CompletionStage<? extends U> other,
            final BiFunction<? super T, ? super U, ? extends V> fn, final Executor executor) {
        return new SafeVertxCompletableFuture<>(vertx, context, super.thenCombineAsync(other, fn, executor));
    }
    
    @Override
    public SafeVertxCompletableFuture<T> whenCompleteAsync(final BiConsumer<? super T, ? super Throwable> action) {
        return new SafeVertxCompletableFuture<>(vertx, context, super.whenCompleteAsync(action, executor));
    }
    
    @Override
    public <U> SafeVertxCompletableFuture<U> handleAsync(final BiFunction<? super T, Throwable, ? extends U> fn) {
        return new SafeVertxCompletableFuture<>(vertx, context, super.handleAsync(fn, executor));
    }
    
    @Override
    public SafeVertxCompletableFuture<T> toCompletableFuture() {
        return this;
    }
    
    @Override
    public T get() throws InterruptedException, ExecutionException {
        checkBlock();
        return super.get();
    }
    
    @Override
    public T get(final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        checkBlock();
        return super.get(timeout, unit);
    }
    
    @Override
    public T join() {
        checkBlock();
        return super.join();
    }
    
    private void complete(final T result, final Throwable error) {
        if(error == null) {
            super.complete(result);
        } else {
            super.completeExceptionally(error);
        }
    }
    
    private void completeFromAsyncResult(final AsyncResult<T> ar) {
        if(ar.succeeded()) {
            super.complete(ar.result());
        } else {
            super.completeExceptionally(ar.cause());
        }
    }
    
    private void checkBlock() {
        if(isDone() || isCompletedExceptionally()) {
            //if we're done/completed we won't block
            return;
        }
        final Context currentContext = Vertx.currentContext();
        if(currentContext != null && Context.isOnEventLoopThread()) {
            if(currentContext.owner() == vertx) {
                throw new IllegalStateException("Possible deadlock detected. Avoid blocking event loop threads");
            } else {
                logger.warn(
                        "Event loop block detected",
                        new Throwable("Blocking method call location"));
            }
        }
    }
}
