import system.MergerEnvironment
import java.util.TreeMap

class MergerImpl<T : Comparable<T>>(
    private val mergerEnvironment: MergerEnvironment<T>,
    prevStepBatches: Map<Int, List<T>>?
) : Merger<T> {
    private val batchesById = HashMap<Int, List<T>>()
    private val infoByValue = TreeMap<T, MutableList<Pair<Int, Int>>>()

    init {
        for (id in 0 until mergerEnvironment.dataHoldersCount) {
            val batch = if ((prevStepBatches != null && prevStepBatches.contains(id)))
                prevStepBatches[id]!! else mergerEnvironment.requestBatch(id)

            if (batch.isNotEmpty()) {
                batchesById[id] = batch
                addValue(batch, id)
            }
        }
    }

    private fun addValue(batch: List<T>, id: Int, i : Int = 0) {
        if (!infoByValue.contains(batch[i])) {
            infoByValue[batch[i]] = ArrayList()
        }
        infoByValue[batch[i]]!!.add(Pair(i, id))
    }

    override fun mergeStep(): T? {
        val res = infoByValue.firstEntry()
        if (res == null) return null

        val (key, info) = res
        val (index, id) = info.removeLast()
        if (index + 1 == batchesById[id]!!.size) {
            batchesById[id] = mergerEnvironment.requestBatch(id)
            if (batchesById[id]!!.isNotEmpty()) {
                addValue(batchesById[id]!!, id)
            }
        } else {
            addValue(batchesById[id]!!, id, index + 1)
        }

        if (info.isEmpty()) {
            infoByValue.remove(key)
        }

        return key
    }

    override fun getRemainingBatches(): Map<Int, List<T>> {
        val res = HashMap<Int, List<T>>()
        for ((_, info) in infoByValue) {
            for ((index, id) in info) {
                val batch = batchesById[id]!!
                res[id] = batch.subList(index, batch.size)
            }
        }
        return res
    }
}