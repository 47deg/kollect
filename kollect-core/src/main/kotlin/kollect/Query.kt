package kollect

import arrow.core.*
import arrow.deriving
import arrow.higherkind
import arrow.typeclasses.Applicative
import arrow.typeclasses.Functor
import java.time.Duration

val Infinite: Duration = Duration.ofDays(Long.MAX_VALUE)

typealias QueryCallback<A> = (A) -> Unit
typealias QueryErrorback = (Throwable) -> Unit

@higherkind
@deriving(Functor::class, Applicative::class)
sealed class Query<A> : QueryOf<A> {

    fun <B> ap(ff: QueryOf<(A) -> B>): Query<B> =
        Ap(this, ff.fix())

    fun <B> map(f: (A) -> B): Query<B> = ap(pure(f))

    companion object {

        fun <A> pure(x: A): Query<A> = Sync(Eval.now(x))

        fun <A> eval(e: Eval<A>): Query<A> = Sync(e)

        fun <A> sync(f: () -> A): Query<A> = Sync(Eval.later(f))

        fun <A> async(action: (QueryCallback<A>, QueryErrorback) -> Unit,
                      timeout: Duration = Infinite): Query<A> = Async(action, timeout)

    }
}

/** A query that can be satisfied synchronously. **/
data class Sync<A>(val action: Eval<A>) : Query<A>()

/** A query that can only be satisfied asynchronously. **/
data class Async<A>(
        val action: (QueryCallback<A>, QueryErrorback) -> Unit,
        val timeout: Duration) : Query<A>()

data class Ap<A, B>(val fa: Query<A>, val ff: Query<(A) -> B>) : Query<B>()