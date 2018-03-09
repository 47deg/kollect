package kollect

import arrow.core.ForTry
import arrow.core.Tuple2
import arrow.effects.ForIO
import arrow.test.UnitSpec
import arrow.test.laws.MonadErrorLaws
import arrow.test.laws.MonoidLaws
import arrow.test.laws.SemigroupLaws
import arrow.typeclasses.*
import io.kotlintest.KTestJUnitRunner
import io.kotlintest.matchers.shouldNotBe
import org.junit.runner.RunWith

@RunWith(KTestJUnitRunner::class)
class KollectMonadErrorTest : UnitSpec() {

    init {

        "instances can be resolved implicitly" {
            kollectMonadError<ForTry>() shouldNotBe null
            kollectMonadError<ForIO>() shouldNotBe null
        }


//        Currently, the MonadErrorLaws are typed to Throwable
//        testLaws(
//               MonadErrorLaws.laws(kollectMonadError<ForTry>(), Eq.any(), Eq.any())
//        )

    }


}