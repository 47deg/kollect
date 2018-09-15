package kollect

import arrow.Kind
import arrow.core.Either
import arrow.core.None
import arrow.core.Option
import arrow.core.Some
import arrow.core.Tuple2
import arrow.core.getOrElse
import arrow.core.toOption
import arrow.data.NonEmptyList
import arrow.data.foldLeft
import arrow.effects.Concurrent
import arrow.effects.deferred.Deferred
import arrow.higherkind
import arrow.instance
import arrow.typeclasses.Applicative
import arrow.typeclasses.Monad
import arrow.typeclasses.binding
import kollect.arrow.ContextShift
import kollect.arrow.concurrent.Ref
import kollect.arrow.effects.Timer

// Kollect queries
interface KollectRequest

// A query to a remote data source
sealed class KollectQuery<I : Any, A> : KollectRequest {
    abstract val dataSource: DataSource<I, A>
    abstract val identities: NonEmptyList<I>

    data class KollectOne<I : Any, A>(val id: I, val ds: DataSource<I, A>) : KollectQuery<I, A>() {
        override val identities: NonEmptyList<I> = NonEmptyList(id, emptyList())
        override val dataSource: DataSource<I, A> = ds
    }

    data class Batch<I : Any, A>(val ids: NonEmptyList<I>, val ds: DataSource<I, A>) : KollectQuery<I, A>() {
        override val identities: NonEmptyList<I> = ids
        override val dataSource: DataSource<I, A> = ds
    }
}

// Kollect result states
sealed class KollectStatus {
    data class KollectDone<A>(val result: A) : KollectStatus()
    object KollectMissing : KollectStatus()
}

// Kollect errors
sealed class KollectException {
    abstract val environment: Env

    data class MissingIdentity<I : Any, A>(val i: I, val request: KollectQuery<I, A>, override val environment: Env) : KollectException()
    data class UnhandledException(val e: Throwable, override val environment: Env) : KollectException()

    fun toThrowable() = NoStackTrace()
}

// In-progress request
data class BlockedRequest<F>(val request: KollectRequest, val result: (KollectStatus) -> arrow.Kind<F, Unit>)

/* Combines the identities of two `KollectQuery` to the same data source. */
private fun <I : Any, A> combineIdentities(x: KollectQuery<I, A>, y: KollectQuery<I, A>): NonEmptyList<I> =
    y.identities.foldLeft(x.identities) { acc, i ->
        if (acc.contains(i)) acc else NonEmptyList(acc.head, acc.tail + i)
    }

/**
 * Combines two requests to the same data source.
 */
@Suppress("UNCHECKED_CAST")
private fun <I : Any, A, F> combineRequests(MF: Monad<F>, x: BlockedRequest<F>, y: BlockedRequest<F>): BlockedRequest<F> {
    val first = x.request
    val second = y.request
    return when {
        first is KollectQuery.KollectOne<*, *> && second is KollectQuery.KollectOne<*, *> -> {
            val first = (first as KollectQuery.KollectOne<I, A>)
            val second = (second as KollectQuery.KollectOne<I, A>)
            val aId = first.id
            val ds = first.ds
            val anotherId = second.id
            if (aId == anotherId) {
                val newRequest = KollectQuery.KollectOne(aId, ds)
                val newResult = { r: KollectStatus -> MF.run { tupled(x.result(r), y.result(r)).flatMap { MF.just(Unit) } } }
                BlockedRequest(newRequest, newResult)
            } else {
                val newRequest = KollectQuery.Batch(combineIdentities(first, second), ds)
                val newResult = { r: KollectStatus ->
                    when (r) {
                        is KollectStatus.KollectDone<*> -> {
                            r.result as Map<*, *>
                            val xResult = r.result[aId].toOption().map { KollectStatus.KollectDone(it) }.getOrElse { KollectStatus.KollectMissing }
                            val yResult = r.result[anotherId].toOption().map { KollectStatus.KollectDone(it) }.getOrElse { KollectStatus.KollectMissing }
                            MF.run { tupled(x.result(xResult), y.result(yResult)).flatMap { MF.just(Unit) } }
                        }

                        is KollectStatus.KollectMissing ->
                            MF.run { tupled(x.result(r), y.result(r)).flatMap { MF.just(Unit) } }
                    }
                }
                BlockedRequest(newRequest, newResult)
            }
        }
        first is KollectQuery.KollectOne<*, *> && second is KollectQuery.Batch<*, *> -> {
            val first = (first as KollectQuery.KollectOne<I, A>)
            val second = (second as KollectQuery.Batch<I, A>)
            val oneId = first.id
            val ds = first.ds

            val newRequest = KollectQuery.Batch(combineIdentities(first, second), ds)
            val newResult = { r: KollectStatus ->
                when (r) {
                    is KollectStatus.KollectDone<*> -> {
                        r.result as Map<*, *>
                        val oneResult = r.result[oneId].toOption().map { KollectStatus.KollectDone(it) }.getOrElse { KollectStatus.KollectMissing }
                        MF.run { tupled(x.result(oneResult), y.result(r)).flatMap { MF.just(Unit) } }
                    }
                    is KollectStatus.KollectMissing -> MF.run { tupled(x.result(r), y.result(r)).flatMap { MF.just(Unit) } }
                }
            }
            BlockedRequest(newRequest, newResult)
        }
        first is KollectQuery.Batch<*, *> && second is KollectQuery.KollectOne<*, *> -> {
            val first = (first as KollectQuery.Batch<I, A>)
            val second = (second as KollectQuery.KollectOne<I, A>)
            val oneId = second.id
            val ds = first.ds

            val newRequest = KollectQuery.Batch(combineIdentities(first, second), ds)
            val newResult = { r: KollectStatus ->
                when (r) {
                    is KollectStatus.KollectDone<*> -> {
                        r.result as Map<*, *>
                        val oneResult = r.result[oneId].toOption().map { KollectStatus.KollectDone(it) }.getOrElse { KollectStatus.KollectMissing }
                        MF.run { tupled(x.result(r), y.result(oneResult)).flatMap { MF.just(Unit) } }
                    }
                    is KollectStatus.KollectMissing -> MF.run { tupled(x.result(r), y.result(r)).flatMap { MF.just(Unit) } }
                }
            }
            BlockedRequest(newRequest, newResult)
        }
        // first is KollectQuery.Batch<*, *> && second is KollectQuery.Batch<*, *>
        else -> {
            val first = (first as KollectQuery.Batch<I, A>)
            val second = (second as KollectQuery.Batch<I, A>)
            val ds = first.ds

            val newRequest = KollectQuery.Batch(combineIdentities(first, second), ds)
            val newResult = { r: KollectStatus -> MF.run { tupled(x.result(r), y.result(r)).flatMap { MF.just(Unit) } } }
            BlockedRequest(newRequest, newResult)
        }
    }
}

/* A map from data sources to blocked requests used to group requests to the same data source. */
data class RequestMap<F>(val m: Map<DataSource<Any, Any>, BlockedRequest<F>>)

/* Combine two `RequestMap` instances to batch requests to the same data source. */
private fun <I : Any, A, F> combineRequestMaps(MF: Monad<F>, x: RequestMap<F>, y: RequestMap<F>): RequestMap<F> =
    RequestMap(x.m.foldLeft(y.m) { acc, tuple ->
        val combinedReq: BlockedRequest<F> = acc[tuple.key].toOption().fold({ tuple.value }, { combineRequests<I, A, F>(MF, tuple.value, it) })
        acc.filterNot { it.key == tuple.key } + mapOf(tuple.key to combinedReq)
    })

// `Kollect` result data type
sealed class KollectResult<F, A> {
    data class Done<F, A>(val x: A) : KollectResult<F, A>()
    data class Blocked<F, A>(val rs: RequestMap<F>, val cont: Kollect<F, A>) : KollectResult<F, A>()
    data class Throw<F, A>(val e: (Env) -> KollectException) : KollectResult<F, A>()
}

// Kollect data type
@higherkind
sealed class Kollect<F, A> : KollectOf<F, A> {

    abstract val run: arrow.Kind<F, KollectResult<F, A>>

    data class Unkollect<F, A>(override val run: arrow.Kind<F, KollectResult<F, A>>) : Kollect<F, A>()

    companion object {
        /**
         * Lift a plain value to the Kollect monad.
         */
        fun <F, A> pure(AF: Applicative<F>, a: A): Kollect<F, A> = Unkollect(AF.just(KollectResult.Done(a)))

        fun <F, A> exception(AF: Applicative<F>, e: (Env) -> KollectException): Kollect<F, A> = Unkollect(AF.just(KollectResult.Throw(e)))

        fun <F, A> error(AF: Applicative<F>, e: Throwable): Kollect<F, A> = exception(AF) { env -> KollectException.UnhandledException(e, env) }

        @Suppress("UNCHECKED_CAST")
        operator fun <F, I : Any, A> invoke(AF: Concurrent<F>, id: I, ds: DataSource<I, A>): Kollect<F, A> =
            Unkollect(AF.binding {
                val deferred = Deferred<F, KollectStatus>(AF) as Deferred<F, KollectStatus>
                val request = KollectQuery.KollectOne(id, ds)
                val result = { a: KollectStatus -> deferred.complete(a) }
                val blocked = BlockedRequest(request, result)
                val anyDs = ds as DataSource<Any, Any>
                val blockedRequest = RequestMap(mapOf(anyDs to blocked))

                KollectResult.Blocked(blockedRequest, Unkollect(
                    deferred.get().flatMap {
                        when (it) {
                            is KollectStatus.KollectDone<*> -> AF.just(KollectResult.Done<F, A>(it.result as A))
                            is KollectStatus.KollectMissing -> AF.just(KollectResult.Throw<F, A> { env ->
                                KollectException.MissingIdentity(id, request, env)
                            })
                        }
                    }
                ))
            })

        /**
         * Run a `Kollect`, the result in the `F` monad.
         */
        fun <F> run(): KollectRunner<F> = KollectRunner()

        class KollectRunner<F>(private val dummy: Boolean = true) : Any() {
            operator fun <A> invoke(
                C: Concurrent<F>,
                CS: ContextShift<F>,
                T: Timer<F>,
                fa: Kollect<F, A>,
                cache: DataSourceCache = InMemoryCache.empty()
            ): Kind<F, A> = C.binding {
                val cacheRef = Ref.of(C, cache).bind()
                val result = performRun(C, CS, T, fa, cacheRef, None).bind()
                result
            }
        }

        /**
         * Run a `Fetch`, the environment and the result in the `F` monad.
         */
        fun <F> runEnv(): KollectRunnerEnv<F> = KollectRunnerEnv()

        class KollectRunnerEnv<F>(private val dummy: Boolean = true) : Any() {
            operator fun <A> invoke(
                C: Concurrent<F>,
                CS: ContextShift<F>,
                T: Timer<F>,
                fa: Kollect<F, A>,
                cache: DataSourceCache = InMemoryCache.empty()
            ): Kind<F, Tuple2<Env, A>> = C.binding {
                val env = Ref.of<F, Env>(C, FetchEnv()).bind()
                val cacheRef = Ref.of(C, cache).bind()
                val result = performRun(C, CS, T, fa, cacheRef, Some(env)).bind()
                val e = env.get().bind()

                Tuple2(e, result)
            }
        }

        /**
         * Run a `Fetch`, the cache and the result in the `F` monad.
         */
        fun <F> runCache(): KollectRunnerCache<F> = KollectRunnerCache()

        class KollectRunnerCache<F>(private val dummy: Boolean = true) : Any() {
            operator fun <A> invoke(
                C: Concurrent<F>,
                CS: ContextShift<F>,
                T: Timer<F>,
                fa: Kollect<F, A>,
                cache: DataSourceCache = InMemoryCache.empty()
            ): Kind<F, Tuple2<DataSourceCache, A>> = C.binding {
                val cacheRef = Ref.of(C, cache).bind()
                val result = performRun(C, CS, T, fa, cacheRef, None).bind()
                val c = cacheRef.get().bind()

                Tuple2(c, result)
            }
        }

        // Data fetching

        private fun <F, A> performRun(
            C: Concurrent<F>,
            CS: ContextShift<F>,
            T: Timer<F>,
            fa: Kollect<F, A>,
            cache: Ref<F, DataSourceCache>,
            env: Option<Ref<F, Env>>
        ): Kind<F, A> = C.binding {
            val result = fa.run.bind()
            val value = when (result) {
                is KollectResult.Done -> C.just(result.x)
                is KollectResult.Blocked -> binding {
                    fetchRound<F, A>(C, CS, T, result.rs, cache, env).bind()
                    performRun(C, CS, T, result.cont, cache, env).bind()
                }
                is KollectResult.Throw -> env.fold({
                    C.just(FetchEnv() : Env())
                }, {
                    it.get()
                }).flatMap { e: Env ->
                    C.raiseError<A>(result.e(e).toThrowable())
                }
            }.bind()
            value
        }

        private fun <F, A> fetchRound(
            C: Concurrent<F>,
            CS: ContextShift<F>,
            T: Timer<F>,
            rs: RequestMap<F>,
            cache: Ref<F, DataSourceCache>,
            env: Option<Ref<F, Env>>
        ): Kind<F, Unit> {
            val blocked = rs.m.toList().map { it.second }
            if (blocked.isEmpty()) {
                C.just(Unit)
            } else {
                C.binding {
                    val requests = NonEmptyList.fromListUnsafe(blocked).parTra
                }
            }
//            if (blocked.isEmpty) Applicative[F].unit
//            else
//                for {
//                    requests <- NonEmptyList.fromListUnsafe(blocked).parTraverse(
//                        runBlockedRequest(_, cache, env)
//                    )
//                    performedRequests = requests.foldLeft(List.empty[Request])(_ ++ _)
//                    _ <- if (performedRequests.isEmpty) Applicative[F].unit
//                    else env match {
//                        case Some(e) => e.modify((oldE) => (oldE.evolve(Round(performedRequests)), oldE))
//                        case None => Applicative[F].unit
//                    }
//                } yield ()
        }

//        private def fetchRound[F[_], A](
//        rs: RequestMap[F],
//        cache: Ref[F, DataSourceCache],
//        env: Option[Ref[F, Env]]
//        )(
//        implicit
//        P: Par[F],
//        C: ConcurrentEffect[F],
//        CS: ContextShift[F],
//        T: Timer[F]
//        ): F[Unit] = {
//            val blocked = rs.m.toList.map(_._2)
//            if (blocked.isEmpty) Applicative[F].unit
//            else
//                for {
//                    requests <- NonEmptyList.fromListUnsafe(blocked).parTraverse(
//                        runBlockedRequest(_, cache, env)
//                    )
//                    performedRequests = requests.foldLeft(List.empty[Request])(_ ++ _)
//                    _ <- if (performedRequests.isEmpty) Applicative[F].unit
//                    else env match {
//                        case Some(e) => e.modify((oldE) => (oldE.evolve(Round(performedRequests)), oldE))
//                        case None => Applicative[F].unit
//                    }
//                } yield ()
//        }
//
//        private def runBlockedRequest[F[_], A](
//        blocked: BlockedRequest[F],
//        cache: Ref[F, DataSourceCache],
//        env: Option[Ref[F, Env]]
//        )(
//        implicit
//        P: Par[F],
//        C: ConcurrentEffect[F],
//        CS: ContextShift[F],
//        T: Timer[F]
//        ): F[List[Request]] =
//        blocked.request match {
//            case q @ FetchOne(id, ds) => runFetchOne[F](q, blocked.result, cache, env)
//            case q @ Batch(ids, ds) => runBatch[F](q, blocked.result, cache, env)
//        }
//    }
//
//    private def runFetchOne[F[_]](
//    q: FetchOne[Any, Any],
//    putResult: FetchStatus => F[Unit],
//    cache: Ref[F, DataSourceCache],
//    env: Option[Ref[F, Env]]
//    )(
//    implicit
//    P: Par[F],
//    C: ConcurrentEffect[F],
//    CS: ContextShift[F],
//    T: Timer[F]
//    ): F[List[Request]] =
//    for {
//        c <- cache.get
//        maybeCached <- c.lookup(q.id, q.ds)
//        result <- maybeCached match {
//            // Cached
//            case Some(v) => putResult(FetchDone(v)) >> Applicative[F].pure(Nil)
//
//            // Not cached, must fetch
//            case None => for {
//            startTime <- T.clock.monotonic(MILLISECONDS)
//            o <- q.ds.fetch(q.id)
//            endTime <- T.clock.monotonic(MILLISECONDS)
//            result <- o match {
//                // Fetched
//                case Some(a) => for {
//                newC <- c.insert(q.id, a, q.ds)
//                _ <- cache.set(newC)
//                result <- putResult(FetchDone[Any](a))
//            } yield List(Request(q, startTime, endTime))
//
//                // Missing
//                case None =>
//                putResult(FetchMissing()) >> Applicative[F].pure(List(Request(q, startTime, endTime)))
//            }
//        } yield result
//        }
//    } yield result
//
//    private case class BatchedRequest(
//        batches: List[Batch[Any, Any]],
//    results: Map[Any, Any]
//    )
//
//    private def runBatch[F[_]](
//    q: Batch[Any, Any],
//    putResult: FetchStatus => F[Unit],
//    cache: Ref[F, DataSourceCache],
//    env: Option[Ref[F, Env]]
//    )(
//    implicit
//    P: Par[F],
//    C: ConcurrentEffect[F],
//    CS: ContextShift[F],
//    T: Timer[F]
//    ): F[List[Request]] =
//    for {
//        c <- cache.get
//
//        // Remove cached IDs
//        idLookups <- q.ids.traverse[F, (Any, Option[Any])](
//        (i) => c.lookup(i, q.ds).map( m => (i, m) )
//        )
//        cachedResults = idLookups.collect({
//            case (i, Some(a)) => (i, a)
//        }).toMap
//        uncachedIds = idLookups.collect({
//            case (i, None) => i
//        })
//
//        result <- uncachedIds match {
//            // All cached
//            case Nil => putResult(FetchDone[Map[Any, Any]](cachedResults)) >> Applicative[F].pure(Nil)
//
//            // Some uncached
//            case l@_ => for {
//            startTime <- T.clock.monotonic(MILLISECONDS)
//
//            uncached = NonEmptyList.fromListUnsafe(l)
//            request = Batch(uncached, q.ds)
//
//            batchedRequest <- request.ds.maxBatchSize match {
//                // Unbatched
//                case None =>
//                request.ds.batch[F](uncached).map(BatchedRequest(List(request), _))
//
//                // Batched
//                case Some(batchSize) =>
//                runBatchedRequest[F](request, batchSize, request.ds.batchExecution)
//            }
//
//            endTime <- T.clock.monotonic(MILLISECONDS)
//            resultMap = combineBatchResults(batchedRequest.results, cachedResults)
//
//            updatedCache <- c.insertMany(batchedRequest.results, request.ds)
//            _ <- cache.set(updatedCache)
//
//            result <- putResult(FetchDone[Map[Any, Any]](resultMap))
//
//        } yield batchedRequest.batches.map(Request(_, startTime, endTime))
//        }
//    } yield result
//
//    private def runBatchedRequest[F[_]](
//    q: Batch[Any, Any],
//    batchSize: Int,
//    e: BatchExecution
//    )(
//    implicit
//    P: Par[F],
//    C: ConcurrentEffect[F],
//    CS: ContextShift[F],
//    T: Timer[F]
//    ): F[BatchedRequest] = {
//        val batches = NonEmptyList.fromListUnsafe(
//            q.ids.toList.grouped(batchSize)
//                .map(batchIds => NonEmptyList.fromListUnsafe(batchIds))
//            .toList
//        )
//        val reqs = batches.toList.map(Batch[Any, Any](_, q.ds))
//
//        val results = e match {
//            case Sequentially =>
//            batches.traverse(q.ds.batch[F])
//            case InParallel =>
//            batches.parTraverse(q.ds.batch[F])
//        }
//
//        results.map(_.toList.reduce(combineBatchResults)).map(BatchedRequest(reqs, _))
//    }
//
//    private def combineBatchResults(r: Map[Any, Any], rs: Map[Any, Any]): Map[Any, Any] =
//    r ++ rs


    }
}

// Kollect ops
@instance(Kollect::class)
interface KollectMonad<F, Identity : Any, Result> : Monad<KollectPartialOf<F>> {

    fun MF(): Monad<F>

    override fun <A> just(a: A): Kollect<F, A> = Kollect.Unkollect(MF().just(KollectResult.Done(a)))

    override fun <A, B> Kind<KollectPartialOf<F>, A>.map(f: (A) -> B): Kollect<F, B> =
        Kollect.Unkollect(MF().binding {
            val kollect = this@map.fix().run.bind()
            val result = when (kollect) {
                is KollectResult.Done -> KollectResult.Done<F, B>(f(kollect.x))
                is KollectResult.Blocked -> KollectResult.Blocked(kollect.rs, kollect.cont.map(f))
                is KollectResult.Throw -> KollectResult.Throw<F, B>(kollect.e)
            }
            result
        })

    override fun <A, B> Kind<KollectPartialOf<F>, A>.product(fb: Kind<KollectPartialOf<F>, B>): Kollect<F, Tuple2<A, B>> =
        Kollect.Unkollect(MF().binding {
            val fab = MF().run { tupled(this@product.fix().run, fb.fix().run).bind() }
            val first = fab.a
            val second = fab.b
            val result = when {
                first is KollectResult.Throw -> KollectResult.Throw<F, Tuple2<A, B>>(first.e)
                first is KollectResult.Done && second is KollectResult.Done -> KollectResult.Done(Tuple2(first.x, second.x))
                first is KollectResult.Done && second is KollectResult.Blocked -> KollectResult.Blocked(second.rs, this@product.product(second.cont))
                first is KollectResult.Blocked && second is KollectResult.Done -> KollectResult.Blocked(first.rs, first.cont.product(fb))
                first is KollectResult.Blocked && second is KollectResult.Blocked -> KollectResult.Blocked(combineRequestMaps<Identity, Result, F>(MF(), first.rs, second.rs), first.cont.product(second.cont))
                // second is KollectResult.Throw
                else -> KollectResult.Throw((second as KollectResult.Throw).e)
            }
            result
        })

    override fun <A, B> tailRecM(a: A, f: (A) -> Kind<KollectPartialOf<F>, Either<A, B>>): Kollect<F, B> =
        f(a).flatMap {
            when (it) {
                is Either.Left -> tailRecM(a, f)
                is Either.Right -> just(it.b)
            }
        }.fix()

    override fun <A, B> Kind<KollectPartialOf<F>, A>.flatMap(f: (A) -> Kind<KollectPartialOf<F>, B>): Kollect<F, B> =
        Kollect.Unkollect(MF().binding {
            val kollect = this@flatMap.fix().run.bind()
            val result: Kollect<F, B> = when (kollect) {
                is KollectResult.Done -> f(kollect.x).fix()
                is KollectResult.Throw -> Kollect.Unkollect(MF().just(KollectResult.Throw(kollect.e)))
                // kollect is KollectResult.Blocked
                else -> Kollect.Unkollect(MF().just(KollectResult.Blocked((kollect as KollectResult.Blocked).rs, kollect.cont.flatMap(f))))
            }
            result.run.bind()
        })
}
