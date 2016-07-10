require('proof')(21, prove)

function prove (assert) {
    var Legislator = require('../legislator')

    function dump (legislator) {
        legislator.log.each(function (entry) { console.log(entry) })
    }

    var time = 0

    var options = {
        parliamentSize: 5,
        ping: 1,
        timeout: 3,
        naturalized: true,
        scheduler: { timerless: true }
    }

    var legislators = [ new Legislator(1, '0', time, options) ]
    legislators[0].bootstrap(time, { location: '0' })

    function receive (legislator, outbox, failures) {
        failures || (failures = {})
        if (outbox.length == 0) {
            return false
        }
        var send = outbox.shift(), responses = {}
        send.route.forEach(function (id) {
            var legislator = legislators[id]
            if (failures[id] != 'request' && failures[id] != 'isolate') {
                responses[id] = legislator.receive(time, send, send.messages)
            }
            if (failures[id] == 'response') {
                delete responses[id]
            }
        })
        legislator.sent(time, send, responses)
        return true
    }

    function send (legislator, failures) {
        failures || (failures = {})
        var sent = false
        var outbox = legislator.synchronize(time)
        if (outbox.length == 0) {
            var consensus = legislator.consensus(time)
            if (consensus) {
                outbox = [ consensus ]
            }
        }
        while (receive(legislator, outbox, failures)) {
            sent = true
        }
        return sent
    }

    function tick (failures) {
        failures || (failures = {})
        var ticked = true
        while (ticked) {
            ticked = false
            legislators.forEach(function (legislator) {
                if (failures[legislator.id] != 'isolate') {
                    legislator.scheduler.check(time)
                    while (send(legislator, failures)) {
                        ticked = true
                    }
                }
            })
        }
    }

    tick()

    assert(legislators[0].government, {
        majority: [ '0' ],
        minority: [],
        constituents: [],
        promise: '1/0'
    }, 'bootstrap')

    legislators.push(new Legislator(1, '1', time, options))

    assert(legislators[0].immigrate(time, 1, '1', legislators[1].cookie, { location: '1' }).enqueued, 'immigrate')

    tick({ 1: 'request' })

    time++

    assert(legislators[0].scheduler.check(time), 'ping missed')

    tick({ 1: 'request' })

    time++

    legislators[0].scheduler.check(time)

    tick()

    assert(legislators[0].government, {
        majority: [ '0' ],
        minority: [],
        immigrate: { id: '1', properties: { location: '1' }, cookie: 0 },
        constituents: [ '1' ],
        promise: '2/0'
    }, 'leader and constituent pair')

    assert(legislators[1].log.size, 2, 'synchronized')
    assert(legislators[1].properties, {
        '0': { location: '0', immigrated: '1/0' },
        '1': { location: '1', immigrated: '2/0' }
    }, 'citizens')

    legislators.push(new Legislator(1, '2', time, options))
    legislators[0].enqueue(time, 1, { type: 'enqueue', value: 1 })
    legislators[0].immigrate(time, 1, '2', legislators[2].cookie, { location: '2' })

    tick()

    assert(legislators[0].government, {
        majority: [ '0', '1' ],
        minority: [ '2' ],
        constituents: [],
        promise: '4/0'
    }, 'three member parliament')

    assert(legislators[2].log.size, 4, 'synchronized')

    assert(legislators[1].enqueue(time, 1, {}).leader, '0', 'post not leader')

    legislators[0]._whenCollapse()
    legislators[1]._whenCollapse()

    assert(!legislators[0].enqueue(time, 1, {}).enqueued, 'post collapsed')

    tick()

    assert(legislators[0].government, {
        majority: [ '0', '1' ],
        minority: [ '2' ],
        constituents: [],
        promise: '5/0'
    }, 'recover from collapse')

    legislators[0].peers[1].timeout = 1

    legislators[0]._whenKeepAlive(time)

    tick()

    assert(legislators[0].peers[1].timeout, 0, 'liveness pulse')

    legislators[1]._whenPing(time, '2')

    assert(legislators[1].peers[2].timeout, 1, 'liveness ping timeout set')

    tick()

    assert(legislators[1].peers[2].timeout, 0, 'liveness ping resolved')

    delete legislators[1].peers[2]

    legislators[1]._whenPing(time, '2')

    tick()

    assert(legislators[1].peers[2].timeout, 0, 'liveness ping materialized')

    legislators.push(new Legislator(1, '3', time, options))
    legislators[0].immigrate(time, 1, '3', legislators[3].cookie, { location: '3' })
    legislators.push(new Legislator(1, '4', time, options))
    legislators[0].immigrate(time, 1, '4', legislators[4].cookie, { location: '4' })
    legislators[0].enqueue(time, 1, { type: 'enqueue', value: 2 })

    while (send(legislators[0]));

    assert(legislators[3].log.size, 1, 'log before naturalization')

    tick()

    assert(legislators[3].log.size, 4, 'log after naturalization')

    legislators[0].enqueue(time, 1, { type: 'enqueue', value: 2 })
    legislators[0].enqueue(time, 1, { type: 'enqueue', value: 3 })

    tick()

    // One more post to propagate the pings to the new memebers back to the
    // leader. TODO Do this by advancing clock to test pings.
    legislators[0].enqueue(time, 1, { type: 'enqueue', value: 3 })

    tick()

    // TODO Always include exiles and naturalization empty and null by default.
    assert(legislators[0].government, {
        majority: [ '0', '1', '2' ],
        minority: [ '3', '4' ],
        constituents: [],
        promise: '8/0'
    }, 'five member parliament')

    legislators[0].enqueue(time, 1, { type: 'enqueue', value: 3 })

    legislators[1].collapse()

    send(legislators[1])

    var consensus = legislators[1].consensus(time)

    tick({ 1: 'isolate' })

    assert(legislators[0].government, {
        majority: [ '0', '2', '3' ],
        minority: [ '1', '4' ],
        constituents: [],
        promise: '9/0'
    }, 'recover from isolation')

    time++
    legislators[2].scheduler.check(time)
    tick()

    receive(legislators[1], [ consensus ])

    // Test inability to create new government because of lack of majority.
    legislators[0].collapse()

    assert(legislators[0].consensus(), null, 'cannot choose leaders')

    tick()

    // Immigrate, but then restart, and assert that the restarted legislator
    // does not immigrate. (I don't see a test for success here.)
    legislators.push(new Legislator(1, '5', time, options))
    legislators[0].immigrate(time, 1, '5', legislators[5].cookie, { location: '5' })

    tick({ 5: 'isolate' })

    time++

    legislators[1].scheduler.check(time)
    send(legislators[1])
    legislators[5] = new Legislator(1, '5', time, options)
    tick()

    legislators[0].collapse()
    send(legislators[0])
    send(legislators[0])
    legislators[2].collapse()
    tick({ 0: 'isolate' })
    tick()

    assert(legislators[2].islandId, 1, 'island id')
}
