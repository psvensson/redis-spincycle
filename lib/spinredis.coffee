uuid      = require('node-uuid')
$q        = require('node-promise')
lru       = require('lru')

debug = process.env['DEBUG']

class spinredis

  constructor: (dbUrl) ->
    @subscribers         = []
    @objsubscribers      = []
    @objectsSubscribedTo = []

    @outstandingMessages = []
    @modelcache          = []

    @seenMessages        = []
    @sessionId           = null
    @objects             = new lru(1000) # have maximum 1000 objects in cache

    if debug then console.log 'redis-spincycle dbUrl = '+dbUrl
    rhost = dbUrl or process.env['REDIS_PORT_6379_TCP_ADDR'] or '127.0.0.1'
    rport = process.env['REDIS_PORT_6379_TCP_PORT'] or '6379'

    @sendredis           = require('redis').createClient(rport, rhost)
    @listenredis         = require('redis').createClient(rport, rhost)

    @listenredis.on 'error', (err) ->
      console.log 'spinredis listen ERROR: '+err

    @sendredis.on 'error', (err) ->
      console.log 'spinredis send ERROR: '+err



    @subscribers['OBJECT_UPDATE'] = [ (obj) ->
      #console.log 'spinclient +++++++++ obj update message router got obj'
      #console.dir(obj);
      @subscribers = objsubscribers[obj.id] or []
      for k,v of @subscribers
        #console.log 'updating subscriber to @objects updates on id '+k
        if not @objects.get(obj.id)
          @objects.set(obj.id, obj)
        else
          o = @objects.get(obj.id)
          for prop, val of obj
            o[prop] = val
        v obj
    ]

    @setup()

  failed: (msg)->
    console.log 'spinclient message failed!! '+msg

  setSessionId: (id) ->
    if(id)
      console.log '++++++++++++++++++++++++++++++++++++++ spinclient setting session id to '+id
      @sessionId = id

  dumpOutstanding: ()->
    console.log '-------------------------------- '+@outstandingMessages.length+' outstanding messages ---------------------------------'
    @outstandingMessages.forEach (os)->
      console.log os.messageId+' -> '+os.target+' - '+os.d
    console.log '-----------------------------------------------------------------------------------------'

  emit: (message) =>
    message.channelID = 'spinchannel_'+@channelID
    if debug then console.log 'redisclient emitting message..'
    if debug then console.dir message
    @sendredis.publish('spinchannel', JSON.stringify(message))

  setup: () =>
    @channelID = uuid.v4()
    @listenredis.subscribe('spinchannel_'+@channelID)

    @listenredis.on 'message', (channel, replystr) =>
      if debug then console.log 'on message got '+replystr
      reply = JSON.parse(replystr)
      status = reply.status
      message = reply.payload
      info = reply.info

      if not @hasSeenThisMessage reply.messageId
        @seenMessages.push(reply.messageId)
        if @seenMessages.length > 10 then @seenMessages.shift()
        if debug then console.log 'redis-spincycle got reply messageId ' + reply.messageId + ' status ' + status + ', info ' + info + ' data ' + message + ' outstandingMessages = '+@outstandingMessages.length
        if debug then @dumpOutstanding()
        #console.dir reply
        index = -1
        if reply.messageId
          i = 0
          while i < @outstandingMessages.length
            index = i
            detail = @outstandingMessages[i]
            if detail and not detail.delivered and detail.messageId == reply.messageId
              if reply.status == 'FAILURE' or reply.status == 'NOT_ALLOWED'
                console.log 'spinclient message FAILURE'
                console.dir reply
                detail.d.reject reply
                break
              else
                #console.log 'delivering message '+message+' reply to '+detail.target+' to '+reply.messageId
                detail.d.resolve(message)
                break
              detail.delivered = true
            i++
          if index > -1
            @outstandingMessages.splice index, 1
        else
          @subscribers = @subscribers[info]
          if @subscribers
            @subscribers.forEach (listener) ->
              listener message
          else
            if debug then console.log 'no subscribers for message ' + message
            if debug then console.dir reply
      else
        if debug then console.log '-- skipped resent message '+reply.messageId

  hasSeenThisMessage: (messageId) =>
    @seenMessages.some (mid) -> messageId == mid

  registerListener: (detail) =>
    console.log 'spinclient::registerListener called for '+detail.message
    @subscribers = @subscribers[detail.message] or []
    @subscribers.push detail.callback
    @subscribers[detail.message] = @subscribers

  registerObjectsSubscriber: (detail) =>
    d = $q.defer()
    sid = uuid.v4()
    localsubs = @objectsSubscribedTo[detail.id]
    console.log 'register@objectsSubscriber localsubs is'
    console.dir localsubs
    if not localsubs
      localsubs = []
      console.log 'no local subs, so get the original server-side subscription for id '+detail.id
      # actually set up subscription, once for each @objects
      @registerObjectsSubscriber({id: detail.id, type: detail.type, cb: (updatedobj) ->
        console.log '-- register@objectsSubscriber getting obj update callback for '+detail.id
        lsubs = @objectsSubscribedTo[detail.id]
        #console.dir(lsubs)
        for k,v of lsubs
          if (v.cb)
            console.log '--*****--*****-- calling back @objects update to local sid --****--*****-- '+k
            v.cb updatedobj
      }).then (remotesid) =>
        localsubs['remotesid'] = remotesid
        localsubs[sid] = detail
        console.log '-- adding local callback listener to @objects updates for '+detail.id+' local sid = '+sid+' remotesid = '+remotesid
        @objectsSubscribedTo[detail.id] = localsubs
        d.resolve(sid)
    return d.promise

  registerObjectsSubscriber: (detail) =>
    d = $q.defer()
    console.log 'message-router registering subscriber for @objects ' + detail.id + ' type ' + detail.type
    @subscribers = @objsubscribers[detail.id] or []

    @emitMessage({target: 'registerForUpdatesOn', obj: {id: detail.id, type: detail.type} }).then(
      (reply)=>
        console.log 'server subscription id for id '+detail.id+' is '+reply
        @subscribers[reply] = detail.cb
        @objsubscribers[detail.id] = @subscribers
        d.resolve(reply)
    ,(reply)=>
      @failed(reply)
    )
    return d.promise

  deRegisterObjectsSubscriber: (sid, o) =>
    localsubs = @objectssSubscribedTo[o.id] or []
    if localsubs[sid]
      console.log 'deregistering local updates for @objects '+o.id
      delete localsubs[sid]
      count = 0
      for k,v in localsubs
        count++
      if count == 1 # only remotesid property left
        @_deRegisterObjectsSubscriber('remotesid', o)

  _degisterObjectsSubscriber: (sid, o) =>
    @subscribers = @objsubscribers[o.id] or []
    if @subscribers and @subscribers[sid]
      delete @subscribers[sid]
      @objsubscribers[o.id] = @subscribers
      @emitMessage({target: 'deRegisterForUpdatesOn', id:o.id, type: o.type, listenerid: sid } ).then (reply)->
        console.log 'deregistering server updates for @objects '+o.id

  emitMessage : (detail) =>
    if debug then console.log 'emitMessage called'
    #console.dir detail
    d = $q.defer()
    detail.messageId = uuid.v4()
    detail.sessionId = detail.sessionId or  @sessionId
    detail.d = d
    @outstandingMessages.push detail
    if debug then console.log 'saving outstanding reply to messageId '+detail.messageId+' and @sessionId '+detail.sessionId
    @emit detail

    return d.promise

# ------------------------------------------------------------------------------------------------------------------

  getModelFor: (type) =>
    d = $q.defer()
    if @modelcache[type]
      d.resolve(@modelcache[type])
    else
      @emitMessage({target:'getModelFor', modelname: type}).then((model)->
        @modelcache[type] = model
        d.resolve(model))
    return d.promise

  listTargets: () =>
    d = $q.defer()
    @emitMessage({target:'listcommands'}).then((targets)-> d.resolve(targets))
    return d.promise

  flattenModel: (model) =>
    rv = {}
    for k,v of model
      if angular.isArray(v)
        rv[k] = v.map (e) -> e.id
      else
        rv[k] = v
    return rv


module.exports = spinredis
