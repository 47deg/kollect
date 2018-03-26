package kollect

import arrow.Kind
import arrow.core.Tuple2
import arrow.data.NonEmptyList
import arrow.free.*
import arrow.free.instances.FreeMonadInstance

abstract class NoStackTrace : Throwable() {
    override fun fillInStackTrace(): Throwable = this
}

sealed class KollectError : NoStackTrace() {
    abstract val env: Env
}

data class NotFound(override val env: Env, val request: KollectOp.KollectOne<Any, Any>) : KollectError()
data class MissingIdentities(override val env: Env, val missing: Map<DataSourceName, List<Any>>) : KollectError()

data class UnhandledException(override val env: Env, val err: Throwable) : KollectError()

interface KollectRequest

interface KollectQuery<I : Any, A> : KollectRequest {
    abstract fun dataSource(): DataSource<I, A>
    abstract fun identities(): NonEmptyList<I>
}

/**
 * Primitive operations in the Kollect Free monad.
 */
sealed class KollectOp<out A> : Kind<KollectOp.F, A> {

    class F private constructor()

    data class Thrown<A>(val err: Throwable) : KollectOp<A>()

    data class Join<A, B>(val fl: Kollect<A>, val fr: Kollect<B>) : KollectOp<Tuple2<A, B>>()

    data class Concurrent(val queries: NonEmptyList<KollectQuery<Any, Any>>) : KollectOp<InMemoryCache>(), KollectRequest

    data class KollectOne<I : Any, A>(val id: I, val ds: DataSource<I, A>) : KollectOp<A>(), KollectQuery<I, A> {
        override fun dataSource(): DataSource<I, A> = ds
        override fun identities(): NonEmptyList<I> = NonEmptyList.pure(id)
    }

    data class KollectMany<I : Any, A>(val ids: NonEmptyList<I>, val ds: DataSource<I, A>) : KollectOp<List<A>>(), KollectQuery<I, A> {
        override fun dataSource(): DataSource<I, A> = ds
        override fun identities(): NonEmptyList<I> = ids
    }

    companion object {

        val kollectMonad: FreeMonadInstance<F> = object : FreeMonadInstance<F> {

            override fun <A> pure(a: A): Kollect<A> = KollectOp.pure(a)

            override fun <A, B> ap(fa: FreeOf<F, A>, ff: FreeOf<F, (A) -> B>): Free<F, B> =
                    KollectOp.join(ff.fix(), fa.fix()).map { (f, a) -> f(a) }

            override fun <A, B> map(fa: FreeOf<F, A>, f: (A) -> B): Free<F, B> =
                    fa.fix().map(f)

            override fun <A, B> product(fa: Kind<FreePartialOf<F>, A>, fb: Kind<FreePartialOf<F>, B>): Kind<FreePartialOf<F>, Tuple2<A, B>> =
                    KollectOp.join(fa.fix(), fb.fix())

            override fun <A, B, Z> map2(fa: Kind<FreePartialOf<F>, A>, fb: Kind<FreePartialOf<F>, B>, f: (Tuple2<A, B>) -> Z): Kind<FreePartialOf<F>, Z> =
                    KollectOp.join(fa.fix(), fb.fix()).map(f)
        }

        /**
         * Lift a plain value to the Kollect monad.
         */
        fun <A> pure(a: A): Kollect<A> = Free.pure(a)

        /**
         * Lift an exception to the Kollect monad.
         */
        fun <A> error(e: Throwable) = Free.liftF(KollectOp.Thrown<A>(e))

        /**
         * Given a value that has a related `DataSource` implementation, lift it
         * to the `Kollect` monad. When executing the kollect the data source will be
         * queried and the kollect will return its result.
         */
        fun <I : Any, A> apply(ds: DataSource<I, A>, i: I): Kollect<A> =
                Free.liftF(KollectOp.KollectOne(i, ds))

        /**
         * Given multiple values with a related `DataSource` lift them to the `Kollect` monad.
         */
        fun <I : Any, A> multiple(ds: DataSource<I, A>, i: I, vararg ids: I): Kollect<List<A>> =
                Free.liftF(KollectOp.KollectMany(NonEmptyList(i, ids.toList()), ds))

        /**
         * Transform a list of kollects into a kollect of a list. It implies concurrent execution of kollects.
         */
        fun <I: Any, A> sequence(ids: List<Kollect<A>>): Kollect<List<A>> = traverse(ids, {x -> x})

        /**
         * Apply a kollect-returning function to every element in a list and return a Kollect of the list of
         * results. It implies concurrent execution of kollects.
         */
        fun <A, B> traverse(ids: List<A>, f: (A) -> Kollect<B>): Kollect<List<B>> =
            traverseGrouped(ids, 50, f)

        fun <A, B> traverseGrouped(ids: List<A>, groupLength: Int, f: (A) -> Kollect<B>): Kollect<List<B>> = TODO()

        /**
         * Apply the given function to the result of the two kollects. It implies concurrent execution of kollects.
         */
        fun <A, B, C> map2(f: (Tuple2<A, B>) -> C, fa: Kollect<A>, fb: Kollect<B>): Kollect<C> =
                KollectOp.kollectMonad.map2(fa.fix(), fb.fix(), f).fix()

        /**
         * Join two kollects from any data sources and return a Kollect that returns a tuple with the two
         * results. It implies concurrent execution of kollects.
         */
        fun <A, B> join(fl: Kollect<A>, fr: Kollect<B>): Kollect<Tuple2<A, B>> =
                Free.liftF(KollectOp.Join(fl, fr))

        /**
         * Run a `Kollect` with the given cache, returning a pair of the final environment and result in the monad `F`.
         */
        fun <A> runKollect(fa: Kollect<A>, cache: DataSourceCache = InMemoryCache.empty()): Kind<F, Tuple2<KollectEnv, A>> = TODO()

        /**
         * Run a `Kollect` with the given cache, returning the final environment in the monad `F`.
         */
        fun <A> runEnv(fa: Kollect<A>, cache: DataSourceCache = InMemoryCache.empty()): Kind<F, KollectEnv> = TODO()

        /**
         * Run a `Kollect` with the given cache, the result in the monad `F`.
         */
        fun <A> run(fa: Kollect<A>, cache: DataSourceCache = InMemoryCache.empty()): Kind<F, A> = TODO()

    }
}

fun <A> Kind<KollectOp.F, A>.fix(): KollectOp<A> = this as KollectOp<A>

typealias Kollect<A> = Free<KollectOp.F, A>