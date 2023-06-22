package mutex

/**
 * Distributed mutual exclusion implementation.
 * All functions are called from the single main thread.
 *
 * @author Aleksandr Slastin
 */
class ProcessImpl(private val env: Environment) : Process {
    private var wantCS = false
    private var inCS = false
    private val pendingOk = BooleanArray(env.nProcesses + 1) // pending OK message (to send on unlock)
    private val edges = Array(env.nProcesses + 1) { i ->
        if (i >= env.processId) Edge.CLEAN else Edge.UNAVAILABLE
    }

    override fun onMessage(srcId: Int, message: Message) {
        message.parse {
            val type = readEnum<MsgType>()
            when (type) {
                MsgType.REQ -> {
                    if (edges[srcId] == Edge.CLEAN && !wantCS) {
                        for (i in 1 .. env.nProcesses) {
                            if (i != env.processId && edges[i] == Edge.CLEAN) {
                                edges[i] = Edge.DIRTY
                            }
                        }
                    }
                    if (edges[srcId] == Edge.DIRTY) {
                        edges[srcId] = Edge.UNAVAILABLE
                        send(srcId, MsgType.OK)
                    } else {
                        pendingOk[srcId] = true
                    }
                }
                MsgType.OK -> {
                    edges[srcId] = Edge.CLEAN
                }
            }
            checkCSEnter()
        }
    }

    override fun onLockRequest() {
        check(!wantCS) { "Lock was already requested" }
        wantCS = true
        checkCSEnter()
    }

    override fun onUnlockRequest() {
        check(inCS) { "We are not in critical section" }
        env.unlocked()
        inCS = false
        wantCS = false
        for (i in 1..env.nProcesses) {
            if (i == env.processId) continue
            if (pendingOk[i]) {
                pendingOk[i] = false
                edges[i] = Edge.UNAVAILABLE
                send(i, MsgType.OK)
            } else {
                edges[i] = Edge.DIRTY
            }
        }
    }

    private fun checkCSEnter() {
        if (!wantCS || inCS) return
        for (i in 1..env.nProcesses) {
            if (edges[i] == Edge.UNAVAILABLE) {
                edges[i] = Edge.UNAVAILABLE_SENT
                send(i, MsgType.REQ)
            }
        }
        val countUnavailableSent = edges.count { it == Edge.UNAVAILABLE_SENT }
        if (countUnavailableSent == 0) {
            for (i in 1 .. env.nProcesses) {
                if (edges[i] == Edge.DIRTY) {
                    edges[i] = Edge.CLEAN
                }
            }
            inCS = true
            env.locked()
        }
    }

    private fun send(destId: Int, type: MsgType) {
        env.send(destId) {
            writeEnum(type)
        }
    }

    enum class Edge { UNAVAILABLE, UNAVAILABLE_SENT, CLEAN, DIRTY }

    enum class MsgType { REQ, OK }
}
