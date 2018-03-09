package kollect

import arrow.core.*
import arrow.data.NonEmptyList
import arrow.data.nel
import kollect.arrow.extensions.tupleLeft

typealias DataSourceName = String
typealias DataSourceIdentity = Tuple2<DataSourceName, Any>

sealed class ExecutionType
object Sequential : ExecutionType()
object Parallel : ExecutionType()

/**
 * A `DataSource` is the recipe for fetching a certain identity `I`, which yields
 * results of type `A`.
 */
interface DataSource<I : Any, A> {

    /**
     * The name of the data source.
     */
    fun name(): DataSourceName

    /**
     * Derive a `DataSourceIdentity` from an identity, suitable for storing the result
     * of such identity in a `DataSourceCache`.
     */
    fun identity(i: I): DataSourceIdentity = name() toT i

    /**
     * Fetch one identity, returning a None if it wasn't found.
     */
    fun fetchOne(id: I): Query<Option<A>>

    /**
     * Fetch many identities, returning a mapping from identities to results. If an
     * identity wasn't found, it won't appear in the keys.
     */
    fun fetchMany(ids: NonEmptyList<I>): Query<Map<I, A>>

    /**
     * Use `fetchOne` for implementing of `fetchMany`. Use only when the data
     * source doesn't support batching.
     */
    fun batchingNotSupported(ids: NonEmptyList<I>): Query<Map<I, A>> {
        val fetchOneWithId: (I) -> Query<Option<Tuple2<I, A>>> = { id ->
            fetchOne(id).map { it.tupleLeft(id).fix() }
        }

        return ids.traverse(fetchOneWithId, Query.applicative())
                .fix()
                .map {
                    it.map { it.orNull() }.all
                            .filterNotNull()
                            .map { Pair(it.a, it.b) }.toMap()
                }
    }

    fun batchingOnly(id: I): Query<Option<A>> =
            fetchMany(id.nel()).map { Option.fromNullable(it[id]) }

    fun maxBatchSize(): Option<Int> = None

    fun batchExecution(): ExecutionType = Parallel
}

