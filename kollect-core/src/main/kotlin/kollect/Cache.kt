package kollect

import arrow.core.Option
import arrow.core.Tuple2

/**
 * A `Cache` interface so the users of the library can provide their own cache.
 */
interface DataSourceCache {

    fun <A> update(k: DataSourceIdentity, v: A): DataSourceCache

    fun <A> get(k: DataSourceIdentity): Option<A>

    fun <I : Any, A> getWithDS(ds: DataSource<I, A>): (I) -> Option<A> = { id -> get(ds.identity(id)) }

    fun <I : Any, A> cacheResults(results: Map<I, A>, ds: DataSource<I, A>): DataSourceCache {
        val op: (DataSourceCache, Map.Entry<I, A>) -> DataSourceCache = { dsc, e ->
            dsc.update(ds.identity(e.key), e.value)
        }
        return results.entries.fold(this, op)
    }

    fun <A> contains(k: DataSourceIdentity): Boolean = get<A>(k).isDefined()
}

/**
 * A cache that stores its elements in memory.
 */
data class InMemoryCache(val state: Map<DataSourceIdentity, Any?>) : DataSourceCache {

    override fun <A> get(k: DataSourceIdentity): Option<A> =
        Option.fromNullable(state[k] as A?)

    override fun <A> update(k: DataSourceIdentity, v: A): DataSourceCache =
        copy(state = state.plus(Pair(k, v)))

    companion object {
        fun empty(): InMemoryCache = InMemoryCache(emptyMap())

        operator fun invoke(vararg results: Tuple2<DataSourceIdentity, Any?>): InMemoryCache =
            InMemoryCache(results.fold(emptyMap(), { map, tuple2 -> map.plus(Pair(tuple2.a, tuple2.b)) }))
    }

}