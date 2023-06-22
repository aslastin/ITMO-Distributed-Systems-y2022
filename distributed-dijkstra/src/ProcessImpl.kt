package dijkstra

import dijkstra.messages.*
import dijkstra.system.environment.Environment

class ProcessImpl(private val environment: Environment) : Process {
    private var distance: Long? = null
    private var isInitiator = false
    private var childCount = 0
    private var balance = 0
    private var parentId: Int? = null

    override fun onMessage(srcId: Int, message: Message) {
        when (message) {
            is MessageWithData -> {
                if (distance == null || message.distance < distance!!) {
                    if (parentId != null) {
                        environment.send(parentId!!, DoneChildMessage)
                    }
                    parentId = srcId
                    distance = message.distance
                    environment.send(srcId, ChildMessage)
                    broadcast()
                } else {
                    environment.send(srcId, NonChildMessage)
                }
            }
            is ChildMessage -> {
                --balance
                ++childCount
            }
            is DoneChildMessage -> --childCount
            is NonChildMessage -> --balance
        }
        checkDone()
    }

    override fun getDistance(): Long? = distance

    override fun startComputation() {
        isInitiator = true
        distance = 0
        broadcast()
        checkDone()
    }

    private fun broadcast() {
        check(distance != null) { "Can broadcast only non-null distance" }
        for ((pid, dst) in environment.neighbours) {
            ++balance
            environment.send(pid, MessageWithData(distance!! + dst))
        }
    }

    private fun checkDone() {
        if (childCount == 0 && balance == 0) {
            if (isInitiator) {
                environment.finishExecution()
            } else if (parentId != null) {
                environment.send(parentId!!, DoneChildMessage)
                parentId = null
            }
        }
    }
}
