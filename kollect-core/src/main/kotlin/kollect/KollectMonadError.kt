package kollect

import arrow.HK
import arrow.TC
import arrow.core.Either
import arrow.typeclass
import arrow.typeclasses.*
import arrow.typeclasses.MonadError

@typeclass
interface KollectMonadError<F> : MonadError<F, KollectError>, TC {

    companion object {
        inline fun <reified F> invoke(): KollectMonadError<F> =
                object : KollectMonadError<F> {

                    override fun <A> pure(a: A): HK<F, A> =
                            monadError<F, Throwable>().pure(a)

                    override fun <A, B> flatMap(fa: HK<F, A>, f: (A) -> HK<F, B>): HK<F, B> =
                            monadError<F, Throwable>().flatMap(fa, f)

                    override fun <A, B> tailRecM(a: A, f: (A) -> HK<F, Either<A, B>>): HK<F, B> =
                            monadError<F, Throwable>().tailRecM(a, f)

                    override fun <A> handleErrorWith(fa: HK<F, A>, f: (KollectError) -> HK<F, A>): HK<F, A> =
                            monadError<F, Throwable>().handleErrorWith(fa, { e -> f(e as KollectError) })

                    override fun <A> raiseError(e: KollectError): HK<F, A> =
                            monadError<F, Throwable>().raiseError(e)
                }

    }
}

@typeclass
interface KollectQueryRunner<F>: KollectMonadError<F>, TC {

    fun <A> runQuery(q: Query<A>): HK<F, A>

}