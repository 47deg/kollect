package kollect

import arrow.core.Tuple2
import arrow.test.UnitSpec
import arrow.test.laws.MonoidLaws
import arrow.test.laws.SemigroupLaws
import arrow.typeclasses.*
import io.kotlintest.KTestJUnitRunner
import io.kotlintest.matchers.shouldNotBe
import org.junit.runner.RunWith

@RunWith(KTestJUnitRunner::class)
class InMemoryCacheTest : UnitSpec() {

    val EQ: Eq<InMemoryCache> = object : Eq<InMemoryCache> {
        override fun eqv(a: InMemoryCache, b: InMemoryCache): Boolean = a.state == b.state
    }

    private fun dsCache(value: Int): InMemoryCache = InMemoryCache(Tuple2(Tuple2("ds", "key"), value))

    init {

        "instances can be resolved implicitly" {
            monoid<InMemoryCache>() shouldNotBe null
        }


        testLaws(
                MonoidLaws.laws(InMemoryCache.monoid(), dsCache(1), EQ),
                SemigroupLaws.laws(InMemoryCache.monoid(),
                        dsCache(1),
                        dsCache(2),
                        dsCache(3),
                        EQ)
        )

    }


}