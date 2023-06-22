import system.DataHolderEnvironment
import kotlin.math.max
import kotlin.math.min

class DataHolderImpl<T : Comparable<T>> public constructor(
    keys: List<T>,
    private val dataHolderEnvironment: DataHolderEnvironment
) : DataHolder<T> {
    private var keys: MutableList<T>
    private var check: Int = keys.size
    private var last = check

    init {
        this.keys = ArrayList(keys).reversed() as MutableList<T>
    }

    override fun checkpoint() {
        check = last
    }

    override fun rollBack() {
        last = check
    }

    override fun getBatch(): List<T> {
        val next = max(0, last - dataHolderEnvironment.batchSize)
        val res = keys.subList(next, last).reversed()
        last = next
        return res
    }
}