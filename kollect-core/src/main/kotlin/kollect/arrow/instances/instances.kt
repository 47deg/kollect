package kollect.arrow.instances

import arrow.instance
import arrow.typeclasses.Monoid
import kollect.InMemoryCache

@instance(InMemoryCache::class)
interface InMemoryCacheMonoidInstance: Monoid<InMemoryCache> {

    override fun empty(): InMemoryCache = InMemoryCache.empty()

    override fun combine(a: InMemoryCache, b: InMemoryCache): InMemoryCache =
        InMemoryCache(a.state.plus(b.state))
}