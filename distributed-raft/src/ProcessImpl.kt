package raft

import raft.Message.*
import raft.ProcessType.*
import kotlin.math.min

enum class ProcessType { FOLLOWER, CANDIDATE, LEADER }

/**
 * Raft algorithm implementation.
 * All functions are called from the single main thread.
 *
 * @author Aleksandr Slastin
 */
class ProcessImpl(private val env: Environment) : Process {
    private val storage = env.storage
    private val machine = env.machine

    private var type = FOLLOWER
    private var currentTerm: Int = 0
    private var votedFor: Int? = null

    private var lastLogId = storage.readLastLogId()

    private var countVotes = -1

    private var commitIndex = 0
    private var lastApplied = 0

    private var N = -1
    private var countMatchIndices = -1

    private var nextIndex: Array<Int> = emptyArray()
    private var matchIndex: Array<Int> = emptyArray()

    private val commands: ArrayDeque<Command> = ArrayDeque()

    init {
        val (currentTerm, _) = storage.readPersistentState()
        toFollower(currentTerm, null)
    }

    override fun onTimeout() =
        if (type == LEADER) {
            heartbeat()
        } else {
            toCandidate()
        }

    override fun onClientCommand(command: Command) {
        when (type) {
            FOLLOWER ->
                if (votedFor == null) {
                    commands.add(command)
                } else {
                    env.send(votedFor!!, ClientCommandRpc(currentTerm, command))
                }
            CANDIDATE -> commands.add(command)
            LEADER -> {
                commands.add(command)
                addCommandsToEntries()
            }
        }
    }

    override fun onMessage(srcId: Int, message: Message) {
        val isMessageFromLeader = (message is AppendEntryResult || message is ClientCommandResult)
                && message.term >= currentTerm

        if (currentTerm < message.term || isMessageFromLeader) {
            toFollower(message.term, if (isMessageFromLeader) srcId else null, false)
        }

        when(message) {
            is AppendEntryRpc ->
                processAppendEntryRpc(message.term, srcId, message.leaderCommit, message.prevLogId, message.entry)
            is AppendEntryResult ->
                processAppendEntryResult(srcId, message.lastIndex)
            is RequestVoteRpc ->
                processRequestVoteRpc(message.term, srcId, message.lastLogId)
            is RequestVoteResult ->
                processRequestVoteResult(message.term, message.voteGranted)
            is ClientCommandRpc ->
                onClientCommand(message.command)
            is ClientCommandResult ->
                env.onClientCommandResult(message.result)
        }

        checkLastApplied()

        if (isMessageFromLeader && type == FOLLOWER) {
            env.startTimeout(Timeout.ELECTION_TIMEOUT)
        }
    }

    private fun processAppendEntryRpc(
        term: Int,
        leaderId: Int,
        leaderCommit: Int,
        prevLogId: LogId,
        entry: LogEntry?
    ) {
        if (currentTerm > term) {
            env.send(leaderId, AppendEntryResult(currentTerm, null))
            return
        }

        var lastIndex: Int? = null

        if (prevLogId.index == 0) {
            lastIndex = 0
        } else {
            storage.readLog(prevLogId.index)?.let {
                if (it.id.compareTo(prevLogId) == 0) {
                    if (entry == null) {
                        lastIndex = prevLogId.index
                        return@let
                    }
                    lastLogId = entry.id
                    storage.appendLogEntry(entry)
                    lastIndex = lastLogId.index
                }
            }
        }

        if (leaderCommit > commitIndex) {
            commitIndex = min(leaderCommit, lastLogId.index)
        }

        env.send(leaderId, AppendEntryResult(currentTerm, lastIndex))
    }

    fun processAppendEntryResult(followerId: Int, lastIndex: Int?) {
        if (type != LEADER) return

        if (lastIndex == null) {
            val index = --nextIndex[followerId]
            val prevLogId = storage.readLog(index - 1)!!.id
            val entry = storage.readLog(index)

            env.send(followerId, AppendEntryRpc(currentTerm, prevLogId, commitIndex, entry))
            return
        }

        nextIndex[followerId] = lastIndex + 1
        matchIndex[followerId] = lastIndex

        if (lastIndex >= N) ++countMatchIndices

        while (countMatchIndices > env.nProcesses / 2) {
            commitIndex = N
            restartN()
            for (i in 1 until env.nProcesses) {
                if (i != env.processId) {
                    countMatchIndices += if (matchIndex[i] >= N) 1 else 0
                }
            }
        }

        if (lastIndex + 1 <= lastLogId.index) {
            val prevLogId = storage.readLog(lastIndex)!!.id
            val entry = storage.readLog(lastIndex + 1)
            env.send(followerId, AppendEntryRpc(currentTerm, prevLogId, commitIndex, entry))
        }
    }

    private fun processRequestVoteRpc(term: Int, candidateId: Int, lastLogId: LogId) {
        val voteGranted = term >= currentTerm && votedFor == null && lastLogId.compareTo(this.lastLogId) >= 0
        if (voteGranted) votedFor = candidateId
        env.send(candidateId, RequestVoteResult(currentTerm, voteGranted))
    }

    private fun processRequestVoteResult(term: Int, voteGranted: Boolean) {
        if (type != CANDIDATE || currentTerm != term) return
        countVotes += if (voteGranted) 1 else 0
        if (countVotes > env.nProcesses / 2) toLeader()
    }

    private fun checkLastApplied() {
        while (commitIndex > lastApplied) {
            ++lastApplied
            val entry = storage.readLog(lastApplied)!!
            val result = machine.apply(entry.command)
            if (type == LEADER) {
                env.send(result.commandId, ClientCommandResult(currentTerm, result))
            }
        }
    }

    private fun heartbeat() {
        broadcast(AppendEntryRpc(currentTerm, lastLogId, commitIndex, null))
        env.startTimeout(Timeout.LEADER_HEARTBEAT_PERIOD)
    }

    private fun broadcast(msg: Message) {
        for (i in 1..env.nProcesses) {
            if (i != env.processId) {
                env.send(i, msg)
            }
        }
    }

    private fun toFollower(newTerm: Int, newVotedFor: Int?, setTimeout: Boolean = true) {
        type = FOLLOWER
        currentTerm = newTerm
        votedFor = newVotedFor

        storage.writePersistentState(PersistentState(currentTerm, votedFor))

        sendCommandsToLeader()

        if (setTimeout) {
            env.startTimeout(Timeout.ELECTION_TIMEOUT)
        }
    }

    private fun sendCommandsToLeader() {
        if (votedFor == null || votedFor == env.processId) return
        for (command in commands) {
            env.send(votedFor!!, ClientCommandRpc(currentTerm, command))
        }
        commands.clear()
    }

    private fun toCandidate(setTimeout: Boolean = true) {
        type = CANDIDATE
        countVotes = 1
        ++currentTerm
        votedFor = env.processId

        storage.writePersistentState(PersistentState(currentTerm, votedFor))

        broadcast(RequestVoteRpc(currentTerm, lastLogId))

        if (setTimeout) {
            env.startTimeout(Timeout.ELECTION_TIMEOUT)
        }
    }

    private fun toLeader() {
        type = LEADER

        addCommandsToEntries()

        restartN()

        nextIndex = Array(env.nProcesses + 1) { lastLogId.index + 1 }
        matchIndex = Array(env.nProcesses + 1) { 0 }

        heartbeat()

        for (i in 1 until env.nProcesses) {
            if (i != env.processId) {
                val prevLogId = storage.readLog(lastLogId.index) ?: continue
                val entry = storage.readLog(prevLogId.id.index + 1)
                env.send(i, AppendEntryRpc(currentTerm, prevLogId.id, commitIndex, entry))
            }
        }
    }

    private fun restartN() {
        N = commitIndex + 1
        while (N <= lastLogId.index && storage.readLog(N)!!.id.term != currentTerm) {
            ++N
        }
        countMatchIndices = 0
    }

    private fun addCommandsToEntries() {
        if (type != LEADER) return
        var index = lastLogId.index
        while (commands.isNotEmpty()) {
            val logId = LogId(++index, currentTerm)
            storage.appendLogEntry(LogEntry(logId, commands.removeFirst()))
        }
        lastLogId = storage.readLastLogId()
    }
}
