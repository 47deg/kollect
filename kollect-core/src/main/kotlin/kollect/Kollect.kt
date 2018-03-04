package kollect

import arrow.TC
import arrow.core.Tuple2
import arrow.data.NonEmptyList
import arrow.free.Free
import arrow.higherkind

typealias Kollect<A> = Free<KollectOpHK, A>

sealed class KollectError {
    abstract fun env(): Env
}
//case class NotFound(env: Env, request: KollectOne[_, _]) : KollectError
//case class MissingIdentities(env: Env, missing: Map[DataSourceName, List[Any]]) : KollectError
//case class UnhandledException(env: Env, err: Throwable) : KollectError

sealed class KollectRequest

sealed class KollectQuery<I : Any, A> : KollectRequest() {
    abstract fun dataSource(): DataSource<I, A>
    abstract fun identities(): NonEmptyList<I>
}

/**
 * Primitive operations in the Kollect Free monad.
 */
@higherkind
interface KollectOp<A> : TC

data class KollectOne<I : Any, A>(val id: I, val ds: DataSource<I, A>) : KollectOp<A>, KollectQuery<I, A>() {
    override fun dataSource(): DataSource<I, A> = ds
    override fun identities(): NonEmptyList<I> = NonEmptyList.pure(id)
}

data class KollectMany<I : Any, A>(val ids: NonEmptyList<I>, val ds: DataSource<I, A>) : KollectOp<List<A>>, KollectQuery<I, A>() {
    override fun dataSource(): DataSource<I, A> = ds
    override fun identities(): NonEmptyList<I> = ids
}

data class Concurrent(val queries: NonEmptyList<KollectQuery<Any, Any>>): KollectOp<InMemoryCache>, KollectRequest()

data class Join<A, B>(val fl: Kollect<A>, val fr: Kollect<B>): KollectOp<Tuple2<A, B>>

data class Thrown<A>(val err: Throwable): KollectOp<A>