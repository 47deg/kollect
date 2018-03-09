package kollect

import arrow.Kind
import arrow.TC
import arrow.core.Either
import arrow.effects.ForIO
import arrow.effects.IO
import arrow.effects.monadError
import arrow.typeclass
import arrow.typeclasses.MonadError

@typeclass(syntax = false)
interface KollectMonadError<F> : MonadError<F, KollectError>, TC {

    fun ME(): MonadError<F, Throwable>

    fun <A> runQuery(q: Query<A>): Kind<F, A>

    override fun <A> pure(a: A): Kind<F, A> =
            ME().pure(a)

    override fun <A, B> flatMap(fa: Kind<F, A>, f: (A) -> Kind<F, B>): Kind<F, B> =
            ME().flatMap(fa, f)

    override fun <A, B> tailRecM(a: A, f: (A) -> Kind<F, Either<A, B>>): Kind<F, B> =
            ME().tailRecM(a, f)

    override fun <A> handleErrorWith(fa: Kind<F, A>, f: (KollectError) -> Kind<F, A>): Kind<F, A> =
            ME().handleErrorWith(fa, { e -> f(e as KollectError) })

    override fun <A> raiseError(e: KollectError): Kind<F, A> =
            ME().raiseError(e)
}

object IOKollectMonadErrorInstance : KollectMonadError<ForIO> {
    override fun ME(): MonadError<ForIO, Throwable> = IO.monadError()

    override fun <A> runQuery(q: Query<A>): Kind<ForIO, A> = TODO()
}
