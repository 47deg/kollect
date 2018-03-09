package arrow.core

import arrow.Kind
import arrow.typeclasses.MonadError
import kollect.KollectMonadError
import kollect.Query

object TryKollectMonadErrorInstance : KollectMonadError<ForTry> {
    override fun ME(): MonadError<ForTry, Throwable> = Try.monadError()

    override fun <A> runQuery(q: Query<A>): Kind<ForTry, A> = TODO()
}

object TryKollectMonadErrorInstanceImplicits {
    fun instance(): TryKollectMonadErrorInstance = TryKollectMonadErrorInstance
}