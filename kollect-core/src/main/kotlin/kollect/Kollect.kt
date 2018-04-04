package kollect

import arrow.Kind
import arrow.core.*
import arrow.data.*
import arrow.free.*
import arrow.higherkind
import arrow.instance
import arrow.typeclasses.Applicative

typealias Kollect<A> = Free<ForKollectOp, A>

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
@higherkind
sealed class KollectOp<out A> : KollectOpOf<A> {
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

        /**
         * Lift a plain value to the Kollect monad.
         */
        fun <A> pure(a: A): Kollect<A> = Free.pure(a)

        /**
         * Lift an exception to the Kollect monad.
         */
        fun <A> error(e: Throwable): Free<ForKollectOp, A> = Free.liftF(KollectOp.Thrown(e))

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
        fun <I : Any, A> sequence(ids: List<Kollect<A>>): Kollect<List<A>> = traverse(ids, { x -> x })

        /**
         * Apply a kollect-returning function to every element in a list and return a Kollect of the list of
         * results. It implies concurrent execution of kollects.
         */
        fun <A, B> traverse(ids: List<A>, f: (A) -> Kollect<B>): Kollect<List<B>> =
                traverseGrouped(ids, 50, f)

        fun <A> ListK<A>.grouped(n: Int): ListK<ListK<A>> =
            withIndex()
                    .groupBy { it.index / n }
                    .map { it.value.k().map { it.value } }.k()



        fun <A, B> traverseGrouped(ids: List<A>, groupLength: Int, f: (A) -> Kollect<B>): Kollect<List<B>> {
            val L = ListK.traverse()
            val matched = ids.k().grouped(groupLength)

            fun foldOp(l: List<A>, evalKollectAcc: Eval<Kollect<List<B>>>): Eval<Kollect<List<B>>> =
                    KollectOp.kollectApplicative().map2Eval(
                            L.traverse(l.k(), f, KollectOp.kollectApplicative()).fix(),
                            evalKollectAcc,
                            { tuple -> tuple.a.fix().plus(tuple.b) }).map { it.fix() }

            return when {
                matched.isEmpty() -> KollectOp.pure(emptyList())
                matched.size == 1 -> L.traverse(matched.first().k(), f, KollectOp.kollectApplicative()).fix()
                else -> L.foldRight(matched, Eval.always({ KollectOp.pure(emptyList<B>()) }), { l, e -> foldOp(l, e) }).value()
            }
        }

        /**
         * Apply the given function to the result of the two kollects. It implies concurrent execution of kollects.
         */
        fun <A, B, C> map2(f: (Tuple2<A, B>) -> C, fa: Kollect<A>, fb: Kollect<B>): Kollect<C> =
                KollectOp.kollectApplicative().map2(fa.fix(), fb.fix(), f).fix()

        /**
         * Join two kollects from any data sources and return a Kollect that returns a tuple with the two
         * results. It implies concurrent execution of kollects.
         */
        fun <A, B> join(fl: Kollect<A>, fr: Kollect<B>): Kollect<Tuple2<A, B>> =
                Free.liftF(KollectOp.Join(fl, fr))

        /**
         * Run a `Kollect` with the given cache, returning a pair of the final environment and result in the monad `F`.
         */
        fun <A> runKollect(fa: Kollect<A>, cache: DataSourceCache = InMemoryCache.empty()): Kind<ForKollectOp, Tuple2<KollectEnv, A>> = TODO()

        /**
         * Run a `Kollect` with the given cache, returning the final environment in the monad `F`.
         */
        fun <A> runEnv(fa: Kollect<A>, cache: DataSourceCache = InMemoryCache.empty()): Kind<ForKollectOp, KollectEnv> = TODO()

        /**
         * Run a `Kollect` with the given cache, the result in the monad `F`.
         */
        fun <A> run(fa: Kollect<A>, cache: DataSourceCache = InMemoryCache.empty()): Kind<ForKollectOp, A> = TODO()

    }
}

@instance(KollectOp::class)
interface KollectApplicativeInstance : Applicative<FreePartialOf<ForKollectOp>> {
    override fun <A, B> ap(fa: FreeOf<ForKollectOp, A>, ff: FreeOf<ForKollectOp, (A) -> B>): Free<ForKollectOp, B> =
            KollectOp.join(ff.fix(), fa.fix()).map { (f, a) -> f(a) }

    override fun <A, B> map(fa: FreeOf<ForKollectOp, A>, f: (A) -> B): Free<ForKollectOp, B> =
            fa.fix().map(f)

    override fun <A, B, Z> map2(fa: Kind<FreePartialOf<ForKollectOp>, A>, fb: Kind<FreePartialOf<ForKollectOp>, B>, f: (Tuple2<A, B>) -> Z): Kind<FreePartialOf<ForKollectOp>, Z> =
            KollectOp.join(fa.fix(), fb.fix()).map(f)

    override fun <A, B> product(fa: Kind<FreePartialOf<ForKollectOp>, A>, fb: Kind<FreePartialOf<ForKollectOp>, B>): Kind<FreePartialOf<ForKollectOp>, Tuple2<A, B>> =
            KollectOp.join(fa.fix(), fb.fix())

    override fun <A> pure(a: A): Free<ForKollectOp, A> =
            KollectOp.pure(a)
}


