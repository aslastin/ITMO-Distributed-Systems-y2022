import java.util.TreeMap

class ConsistentHashImpl<K> : ConsistentHash<K> {
    private val vnodesByShard = HashMap<Shard, Set<Int>>()
    private val shardByVnode = HashMap<Int, Shard>()
    private val keysByVnode = TreeMap<Int, TreeMap<Int, List<K>>>()

    override fun getShardByKey(key: K): Shard = shardByVnode[getVnodeNext(key.hashCode())]!!

    override fun addShard(newShard: Shard, vnodeHashes: Set<Int>): Map<Shard, Set<HashRange>> {
        val wasEmpty = keysByVnode.isEmpty()

        vnodesByShard[newShard] = vnodeHashes
        for (vnode in vnodeHashes) {
            shardByVnode[vnode] = newShard
            keysByVnode[vnode] = TreeMap()
        }

        if (wasEmpty) return emptyMap()

        return generateResult(newShard)
    }

    override fun removeShard(shard: Shard): Map<Shard, Set<HashRange>> {
        if (vnodesByShard.size == 1) {
            vnodesByShard.clear()
            shardByVnode.clear()
            keysByVnode.clear()
            return emptyMap()
        }

        val res = generateResult(shard) { _, right, next ->
            keysByVnode.remove(right)
            shardByVnode.remove(right)
        }

        vnodesByShard.remove(shard)

        return res
    }

    private fun generateResult(
        shard: Shard,
        actionPerSegment: (Int, Int, Int) -> Unit = {_, _, _ ->}
    ): Map<Shard, Set<HashRange>> {
        val vnodes = HashSet(vnodesByShard[shard]!!)
        val res = HashMap<Shard, MutableSet<HashRange>>()

        while (vnodes.isNotEmpty()) {
            val cur = findFirstLessOtherShard(vnodes)

            val (range, next) = rangeAndNext(cur, vnodes)

            var left = range.first()
            for (right in range.subList(1, range.size)) {
                actionPerSegment(left, right, cur)
                left = right
            }

            res.computeIfAbsent(shardByVnode[next]!!) { HashSet() }
                .add(HashRange(range.first() + 1, range.last()))
        }

        return res
    }

    private fun rangeAndNext(
        cur: Int,
        vnodes: MutableSet<Int>
    ): Pair<ArrayList<Int>, Int> {
        var next = cur
        val range = ArrayList<Int>()

        do {
            range.add(next)
            vnodes.remove(next)
            next = keysByVnode.higherKey(next) ?: keysByVnode.firstKey()!!
        } while (vnodes.contains(next))

        return Pair(range, next)
    }

    private fun findFirstLessOtherShard(vnodes: MutableSet<Int>): Int {
        var cur = vnodes.first()
        while (vnodes.contains(cur)) {
            cur = getVnodePrev(cur)
        }
        return cur
    }

    private fun getVnodeNext(key: Int): Int = keysByVnode.ceilingKey(key) ?: keysByVnode.firstKey()!!
    private fun getVnodePrev(key: Int): Int = keysByVnode.lowerKey(key) ?: keysByVnode.lastKey()!!

}