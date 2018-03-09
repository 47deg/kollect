package kollect.arrow.extensions

import arrow.*
import arrow.core.*
import arrow.typeclasses.*

fun <F, A, B> Functor<F>.tupleLeft(fa: Kind<F, A>, b: B): Kind<F, Tuple2<B, A>> =
        map(fa, { a -> b toT a })

inline fun <reified F, A, B> Kind<F, A>.tupleLeft(b: B, FF: Functor<F> = functor()): Kind<F, Tuple2<B, A>> =
        FF.tupleLeft(this, b)