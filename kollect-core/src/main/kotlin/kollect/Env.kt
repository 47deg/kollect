package kollect

/**
 * An environment that is passed along during the fetch rounds. It holds the
 * cache and the list of rounds that have been executed.
 */
interface Env {
    val rounds: List<Round>
    val cache: DataSourceCache
    fun evolve(newRound: Round, newCache: DataSourceCache): Env
}

/**
 * A data structure that holds information about a fetch round.
 */
data class Round(
        val cache: DataSourceCache,
        val request: KollectRequest,
        val response: Any,
        val start: Long,
        val end: Long) {
    fun duration(): Double = (end - start) / 1e6
}

/**
 * A concrete implementation of `Env` used in the default Fetch interpreter.
 */
data class KollectEnv(
        override val cache: DataSourceCache,
        override val rounds: List<Round> = emptyList()) : Env {

    override fun evolve(
            newRound: Round,
            newCache: DataSourceCache): KollectEnv =
            copy(rounds = rounds + newRound, cache = newCache)
}
