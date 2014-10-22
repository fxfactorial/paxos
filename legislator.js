var assert = require('assert')
var Monotonic = require('monotonic')
var push = [].push
var RBTree = require('bintrees').RBTree;

var Id = {
    toWords: function (id) {
        var split = id.split('/')
        return [ Monotonic.parse(split[0]), Monotonic.parse(split[1]) ]
    },
    toString: function (id) {
        return Monotonic.toString(id[0]) + '/' + Monotonic.toString(id[1])
    },
    compare: function (a, b, index) {
        a = Id.toWords(a)
        b = Id.toWords(b)
        if (index == null) {
            var compare = Monotonic.compare(a[0], b[0])
            if (compare == 0) {
                return Monotonic.compare(a[1], b[1])
            }
            return compare
        }
        return Monotonic.compare(a[index], b[index])
    },
    compareGovernment: function (a, b) {
        a = Id.toWords(a)
        b = Id.toWords(b)
        return Monotonic.compare(a[0], b[0])
    },
    increment: function (id, index) {
        id = Id.toWords(id)
        var next = [ id[0], id[1] ]
        next[index] = Monotonic.increment(next[index])
        return Id.toString(next)
    }
}

function Legislator (id) {
    this.id = id
    this.idealGovernmentSize = 5
    this.proposal = { id: '0/0' }
    this.promise = { id: '0/0' }
    this.log = new RBTree(function (a, b) { return Id.compare(a.id, b.id) })
    this.restarted = true
    this.government = {
        leader: 0,
        majority: [ 0, 1, 2 ],
        members: [ 0, 1, 2, 3, 4 ]
    }
    this.last = {}
    this.last[id] = {
        learned: '0/0',
        decided: '0/0',
        uniform: '0/0'
    }
    var motion = {}
    this.queue = motion.prev = motion.next = motion

    var entry = this.entry('0/0', {
        id: '0/0',
        value: 0,
        quorum: 1
    })

    entry.learns = [ id ]
    entry.learned = true
    entry.decided = true
    entry.uniform = true
}

Legislator.prototype.bootstrap = function () {
    this.restarted = false
    this.government = {
        leader: this.id,
        majority: [ this.id ],
        members: [ this.id ],
        interim: true
    }
    return this.propose({
        internal: true,
        value: {
            type: 'government',
            to: this.government.majority.slice(),
            from: [ this.id ],
            government: this.government
        }
    })
}

Legislator.dispatch2 = function (legislators, path, messages, logger) {
    var legislator = legislators[path[path.length - 1]],
        proxy, proxies = {}, self, responses = []
    do {
        self = false
        messages.forEach(function (message) {
            var index
            if (~(index = message.to.indexOf(legislator.id))) {
                self = true
                logger(legislator.id, message)
                message.to.splice(index, 1)
                var type = message.type
                var method = 'receive' + type[0].toUpperCase() + type.substring(1)
                push.apply(responses, legislator[method](message))
            }
            if (message.proxy && message.proxy.length) {
                message.to.slice(message.to.indexOf(legislator.id), 1)
                var key = message.proxy.join('.')
                if (!proxies[key]) proxies[key] = []
                proxies[key].push(message)
            } else if (message.to.length) {
                responses.push(message)
            }
        })
        messages = responses
    } while (self)

    var responses = [], invocations = {}
    messages.forEach(function (message) {
        message.to.forEach(function (to) {
            if (!invocations[to]) invocations[to] = []
            invocations[to].push(message)
        })
    })

    for (var key in invocations) {
        Legislator.dispatch2(legislators, path.concat(+key), invocations[key], logger)
    }
}

Legislator.dispatch = function (messages, legislators) {
    var responses = []
    messages.forEach(function (message) {
        var type = message.type
        var method = 'receive' + type[0].toUpperCase() + type.substring(1)
        legislators.forEach(function (legislator) {
            var index
            if (~(index = message.to.indexOf(legislator.id))) {
                message.to.splice(index, 1)
                push.apply(responses, legislator[method](message))
                /*
                if (message.forward && message.forward.length) { // todo: validator.forward(message)
                    var forward = {}
                    for (var key in message) {
                        forward[key] = message[key]
                    }
                    forward.to = [ message.forward[0] ]
                    if (message.forward.length == 1) {
                        delete forward.forward
                    } else {
                        forward.forward = message.forward.slice(1)
                    }
                    responses.push(forward)
                }
                */
            }
        })
        if (message.to.length) {
            responses.push(message)
        }
    })
    var decisions = {}, amalgamated = []
    responses.forEach(function (message) {
        var key = Id.toString(message.id)
        var decision = decisions[key]
        if (!decision) {
            decision = decisions[key] = { messages: [] }
        }
        var previous = decision.messages[message.type]
        if (!previous) {
            previous = decision.messages[message.type] = message
            amalgamated.push(message)
        } else {
            message.from.forEach(function (id) {
                if (!~previous.from.indexOf(id)) {
                    previous.from.push(id)
                }
            })
            message.to.forEach(function (id) {
                if (!~previous.to.indexOf(id)) {
                    previous.to.push(id)
                }
            })
        }
    })
    return amalgamated
}

Legislator.prototype.enqueue = function (value) {
    assert(this.government.leader == this.id, 'not leader')
    var entry = { value: value, prev: this.queue.prev, next: this.queue }
    entry.next.prev = entry
    entry.prev.next = entry
}

Legislator.prototype.propose = function (proposal) {
    this.createProposal(0, proposal)
    return [{
        from: [ this.id ],
        to: this.government.majority.slice(),
        type: 'prepare',
        id: this.proposal.id
    }]
}

Legislator.prototype.receivePrepare = function (message) {
    var compare = Id.compareGovernment(this.promise.id, message.id)

    if (compare == 0) {
        return []
    }

    if (compare < 0) {
        this.promise = { id: message.id }
        return [{
            from: [ this.id ],
            to: message.from,
            type: 'promise',
            id: this.promise.id
        }]
    }

    return [{
        from: [ this.id ],
        to: [ message.from ],
        type: 'promised',
        id: this.promisedId
    }]
}

Legislator.prototype.receivePromise = function (message) {
    return [].concat.apply([], message.from.map(function (id) {
        var compare = Id.compare(this.proposal.id, message.id)

        if (compare != 0) {
            return []
        }

        if (!~this.proposal.quorum.indexOf(id)) {
            return []
        }

        if (!~this.proposal.promises.indexOf(id)) {
            this.proposal.promises.push(id)
        } else {
            // We have already received a promise. Something is probably wrong.
            return []
        }

        if (this.proposal.promises.length == this.proposal.quorum.length) {
            return this.accept()
        }

        return []
    }.bind(this)))
}

Legislator.prototype.createProposal = function (index, prototype) {
    var previous = this.log.max()
    this.proposal = {
        id: Id.increment(this.proposal.id, index),
        internal: !! prototype.internal,
        value: prototype.value,
        quorum: this.government.majority.slice(),
        previous: previous.id,
        promises: [],
        accepts: []
    }
}

Legislator.prototype.accept = function () {
    this.entry(this.proposal.id, {
        quorum: this.government.majority.length,
        value: this.proposal.value
    })
    return [{
        from: [ this.id ],
        to: this.government.majority.slice(),
        proxy: this.government.majority.slice(),
        type: 'accept',
        internal: this.proposal.internal,
        previous: this.proposal.previous,
        quorum: this.government.majority.length,
        id: this.proposal.id,
        value: this.proposal.value
    }, {
        from: [ this.id ],
        to: this.government.majority.slice(),
        type: 'accepted',
        id: this.proposal.id
    }]
}

Legislator.prototype.entry = function (id, message) {
    var entry = this.log.find({ id: id })
    if (!entry) {
        var entry = {
            id: id,
            accepts: [],
            learns: [],
            quorum: message.quorum,
            value: message.value
        }
        this.log.insert(entry)
    }
    ([ 'quorum', 'value', 'previous', 'internal' ]).forEach(function (key) {
        if (entry[key] == null && message[key] != null) {
            entry[key] = message[key]
        }
    })
    return entry
}

Legislator.prototype.receiveAccept = function (message) {
    var compare = Id.compareGovernment(this.promise.id, message.id)
    if (compare > 0) {
        return [{
            type: 'reject'
        }]
    } else if (compare < 0) {
    } else {
        this.entry(message.id, message)
        return [{
            type: 'accepted',
            from: [ this.id ],
            to: this.government.majority.slice(),
            id: message.id
        }]
    }
}

Legislator.prototype.receiveAccepted = function (message) {
    var entry = this.entry(message.id, message), messages = []
    message.from.forEach(function (id) {
        if (!~entry.accepts.indexOf(id)) {
            entry.accepts.push(id)
        }
        if (entry.accepts.length >= entry.quorum && !entry.learned)  {
            entry.learned = true
            if (!this.restarted && ~this.government.majority.indexOf(this.id)) {
                messages.push({
                    from: [ this.id ],
                    to: [ this.government.leader ],
                    type: 'learned',
                    id: message.id
                })
            }
            push.apply(messages, this.dispatchInternal('learn', entry))
        }
    }, this)
    return messages
}

Legislator.prototype.dispatchInternal = function (prefix, entry) {
    if (entry.internal) {
        var type = entry.value.type
        var method = prefix + type[0].toUpperCase() + type.slice(1)
        if (typeof this[method] == 'function') {
            return this[method](entry.id)
        }
    }
    return []
}

Legislator.prototype.markUniform = function () {
    // Start from the last uniform entry.
    var iterator = this.log.findIter({ id: this.last[this.id].uniform }),
        previous = iterator.data(), entry = iterator.next(), id = {}

    this.log.each(function (entry) {
        console.log('ordered', entry.id, !! entry.uniform)
    })

    // Trampoline for a naturally recursive algorithm.
    var f = normal
    while (f = f.call(this));

    // Mark current entry as uniform.
    function uniform () {
        entry.uniform = true
        this.last[this.id].uniform = entry.id
    }

    // Increment our iterator.
    function increment () {
        previous = entry
        entry = iterator.next()
    }

    var iterator = this.log.findIter({ id: this.last[this.id].uniform })
    for (;;) {
        previous = iterator.data(), current = iterator.next()
        if (!current) {
            break
        }
        if (Id.compare(Id.increment(previous.id, 1), current.id) == 0) {
            assert(previous.decided, 'previous must be decided')
            uniform.call(this)
            continue
        }
        trampoline.call(this, this.log.findIter({ id:
    }

    // Mark an entry uniform if it immediately succeeds it's previous entry.
    function normal () {
        while (entry) {
        }
    }

    function decisions () {
        function decision () {
            if (Id.compare(Id.increment(previous.id, 1), entry.id) == 0) {
                increment()
                if (previous.decided || previous.ignored) {
                    uniform.call(this)
                    return decision
                } else {
                    return incomplete
                }
            }
        }
    }

    function trampoline (f, i) {
        while (typeof (f = f.call(this, i)) == 'function');
        return f
    }

    function transition (i) {
        // Read from our iterator.
        var previous = i.data(), current = i.next()

        // If we are not at a goverment transition, then the previous entry
        // cannot be known to be incomplete.
        if (!entry || Id.compare(previous.id, entry.id, 0) == 0) {
            return null
        }

        // Ideally will be looking at the start of a new government.
        previous = i.data(), current = i.next()

        // The next uniform entry would be the start of a new government.
        if (Id.compare(prevoius.id, '0/0', 1) != 0) {
            return null
        }

        // If it did not settle old business, it might have failed.
        if (Id.compare(Id.increment(previous.id, 1), entry.id) == 0) {
            return transition
        }

        // We now have the correct records for the start of a new government,
        // did they become actionable? The we have a continual government.
        if (previous.decided) {
            if (entry.decided) {
                return entry.value.id
            }
            // Perhaps our old business entry is inactionable and this
            // government failed.
            return transition
        }

        // This probably cannot be reached, since every entry we get will be
        // decided except for one that was left inside an legislator.
        return transition
    }

    // Search for proof that an inactionable decision is actually an incomplete
    // final decision of an old goverment.
    function incomplete () {
        // If we are at a goverment transition, then the previous entry cannot
        // be known to be incomplete.
        if (!entry || Id.compare(previous.id, entry.id, 0) == 0) {
            return null
        }

        // Assert that the first entry is an interim goverment entry.
        assert(entry.value.type == 'government')

        // Move to the new business of the new goverment.
        increment()

        // Look for the end of the iterim. If we see a new goverment, this
        // goverment failed.

        var terminate

        government(function () {
            if (entry.value.type == 'terminate') {
                id.terminal = entry.value.id
            }
        }, complete, incomplete)

        function complete () {
            // Reset iterator to last actionable decision of old government.
            rewind(terminate.value.id)

            // Ignore the decision that follows it.
            return ignore
        }
    }

    // Rewind to the entry that follows the given id.
    function rewind (id) {
        iterator = this.log.findIter({ id: id })
        previous = iterator.data()
        entry = iterator.next()
    }

    // Ignore the current entry as unfinished old business of an old government.
    function ignore () {
        assert(previous.uniform, 'previous not uniform')
        assert(!entry.uniform, 'entry is uniform')

        entry.ignored = true

        increment()

        assert(Id.compare(previous.id, entry.id, 0) == 0, 'ignore without transition')

        return transition
    }

    function noop () {
    }

    function government (visit, complete, incomplete) {
        // Assert
        if (Id.toWords(entry.id)[1][0] != 0) {
            return null
        }

        // NOPE: Assert that we are at the start of a government.
        // assert(entry.value.type == 'government', 'not at government')
        // assert(Id.toWords(entry.id)[1][0] == 0, 'not a first order of business')

        // Consume the transitional entry.
        visit.call(this)
        increment()

        // Look for the end of the iterim. If we see a new goverment, this
        // goverment failed.
        var continual
        while (entry && !continual) {
            if (Id.compare(Id.increment(previous.id, 1), entry.id) != 0) {
                return null
            }
            if (Id.compare(previous.id, entry.id, 0) != 0) {
                return incomplete
            }
            if (entry.value.type == 'government') {
                continual = entry
            }
            visit.call(this)
            increment()
        }

        return continual && complete
    }

    // Mark a transition to a new goverment.
    function transition () {
        // Not actually a new government.
        if (Id.compare(previous.id, entry.id, 0) == 0) {
            return null
        }

        // Mark for rewind.
        var mark = previous.id

        // Determine if goverment failed.
        return government.call(this, noop, complete, failed)

        // It did not fail, mark it uniform.
        function continual () {
            rewind.call(this, mark)
            return government.call(this, normal, uniform, noop)
        }

        // It did not fail, mark it ignored.
        function failed () {
            rewind.call(this, mark)
            return government.call(this, transition, function () {
                entry.ignored = true
            }, noop)
        }
    }
}

Legislator.prototype.receiveLearned = function (message) {
    var entry = this.entry(message.id, message), messages = []
    message.from.forEach(function (id) {
        if (!~entry.learns.indexOf(id)) {
            entry.learns.push(id)
        }
        if (entry.learns.length == entry.quorum) {
            entry.decided = true
            this.markUniform()
            push.apply(messages, this.dispatchInternal('decide', entry))
        }
        if (entry.decided && Id.compare(this.proposal.id, message.id) == 0) {
            messages.push({
                from: this.government.majority.slice(),
                to: this.government.majority.filter(function (id) {
                    return id != this.id
                }.bind(this)),
                type: 'learned',
                id: message.id
            })
            if (this.queue.next.value) {
                var next = this.queue.next
                next.prev.next = next.next
                next.next.prev = next.prev
                this.createProposal(1, next.value)
                this.accept()
            }
        }
    }, this)
    return messages
}

Legislator.prototype.learnGovernment = function (id) {
    return this.sync([ this.government.leader ], 0)
    var entry = this.entry(id, {})
    return [{
        type: 'last',
        to: [ entry.value.government.leader ],
        from: [ this.id ],
        id: id,
        learned: this.last[this.id].learned,
        decided: this.last[this.id].decided
    }]
}

Legislator.prototype.sync = function (to, count) {
    return [{
        type: 'synchronize',
        to: to,
        from: [ this.id ],
        count: count,
        last: this.last[this.id]
    }]
}

Legislator.prototype.decideGovernment = function (id) {
    var entry = this.entry(id, {})
    this.government = entry.value.government
    if (this.government.interim) {
        if (this.government.leader == this.id) {
            var majority = this.government.majority.slice()
            majority.sort(function (a, b) {
                return Id.compare(this.last[b].learned, this.last[a].learned)
            }.bind(this))
            var oldBusiness = this.last[majority[0]].learned
            assert(majority[0] == this.id, 'need to catch up')
            this.createProposal(1, {
                internal: true,
                value: {
                    type: 'terminate',
                    id: oldBusiness
                }
            })
            return this.accept()
        }
    }
    return []
}

Legislator.prototype.decideTerminate = function (id) {
    this.government.interim = false
    this.createProposal(1, {
        internal: true,
        value: {
            type: 'government',
            government: this.government
        }
    })
    return this.accept()
}

Legislator.prototype.receiveSynchronize = function (message) {
    assert(message.from.length == 1, 'multi synchronize')

    var id = message.from[0]

    console.log(message)
    if (message.last) {
        this.last[id] = message.last
    }

    var messages = []

    if (message.count) {
        messages.push({
            from: [ this.id ],
            to: [ id ],
            type: 'synchronize',
            count: 0,
            last: this.last[id],
            government: this.government
        })

        message.count--
        messages.push(createLearned([ id ], this.log.find({ id: this.last[this.id].uniform })))

        console.log(this.last, id, message)
        var iterator = this.log.findIter({ id: this.last[id].uniform }), entry
        if (iterator == null) throw new Error('HC SVNT DRACONES') // TODO
        var count = 20
        while (count-- && (entry = iterator.next()) != null) {
            if (entry.uniform) {
                messages.push(createLearned([ id ], entry))
            } else if (!entry.ignored) {
                break
            }
        }
    }

    return messages

    function createLearned (to, entry) {
        return {
            type: 'learned',
            to: to,
            from: entry.learns.slice(),
            id: entry.id,
            quorum: entry.quorum,
            value: entry.value
        }
    }
}


module.exports = Legislator
