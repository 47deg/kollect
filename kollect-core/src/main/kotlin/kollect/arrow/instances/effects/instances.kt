package arrow.effects

import arrow.Kind
import arrow.typeclasses.MonadError
import kollect.KollectMonadError
import kollect.Query

object IOKollectMonadErrorInstance : KollectMonadError<ForIO> {
    override fun ME(): MonadError<ForIO, Throwable> = IO.monadError()

    override fun <A> runQuery(q: Query<A>): Kind<ForIO, A> = TODO()
}

object IOKollectMonadErrorInstanceImplicits {
    fun instance(): IOKollectMonadErrorInstance = IOKollectMonadErrorInstance
}