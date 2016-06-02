uuid = require('node-uuid')
$q = require('node-promise')
lru = require('lru')

debug = process.env['DEBUG']

opts =
  max: 1000
  maxAgeInMilliseconds: 1000 * 60 * 60 * 24 * 4 # 4 days timeout of objects no matter what

class spinredis

  constructor: (dbUrl) ->
    console.log 'spinclient +++++++++ constructor called +++++++++++'
    @open = false
    @subscribers = []
    @objsubscribers = []
    @objectsSubscribedTo = []

    @outstandingMessages = []
    @modelcache = []

    @seenMessages = []
    @sessionId = null
    @objects = new lru(opts)

    @savedMessagesInCaseOfRetries = new lru({max:1000, maxAgeInMilliseconds: 5000})

    if debug then console.log 'redis-spincycle dbUrl = ' + dbUrl
    rhost = dbUrl or process.env['REDIS_PORT_6379_TCP_ADDR'] or '127.0.0.1'
    rport = process.env['REDIS_PORT_6379_TCP_PORT'] or '6379'

    @sendredis = require('redis').createClient(rport, rhost)
    @listenredis = require('redis').createClient(rport, rhost)

    @listenredis.on 'error', (err) ->
      console.log 'spinredis listen ERROR: ' + err

    @listenredis.on 'end', (err) ->
      console.log 'spinredis listen end event: ' + err

    @sendredis.on 'error', (err) ->
      console.log 'spinredis send ERROR: ' + err

    @sendredis.on 'end', (err) ->
      console.log 'spinredis send end event: ' + err


    @subscribers['OBJECT_UPDATE'] = [(obj) =>
      console.log 'spinredis +++++++++ obj update message router got obj '+obj.id+' of type '+obj.type
      console.dir(obj);
      #console.dir(@objsubscribers)
      objsubs = @objsubscribers[obj.id] or []
      for k,v of objsubs
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
    console.log 'spinclient message failed!! ' + msg

  setSessionId: (id) ->
    if(id)
      console.log '++++++++++++++++++++++++++++++++++++++ spinclient setting session id to ' + id
      @sessionId = id

  dumpOutstanding: ()->
    console.log '-------------------------------- ' + @outstandingMessages.length + ' outstanding messages ---------------------------------'
    @outstandingMessages.forEach (os)->
      console.log os.messageId + ' -> ' + os.target + ' - ' + os.d
    console.log '-----------------------------------------------------------------------------------------'

  emit: (message) =>
    message.channelID = 'spinchannel_' + @channelID
    if @open
      _emit(message)
    else
      console.log 'spincycle is not awake at other end yet...'
      setTimeout(
        ()=>
          @emit(message)
      ,200+parseInt(Math.random()*100)
      )

  _emit:(message)=>
    if debug then console.log 'redisclient emitting message..'
    if debug then console.dir message
    @savedMessagesInCaseOfRetries.set(message.messageId, message)
    @sendredis.publish('spinchannel', JSON.stringify(message))

  openChannel:()=>
    # 'list of available targets'

    if not @open
      @sendredis.publish('spinchannel', JSON.stringify({target: 'listcommands', channelID: 'spinchannel_'+@channelID, messageId: uuid.v4()}))
      setTimeout(
        ()=>
          @openChannel()
        ,250
      )

  setup: () =>
    @channelID = uuid.v4()
    @listenredis.subscribe('spinchannel_' + @channelID)
    @openChannel()

    @listenredis.on 'message', (channel, replystr) =>
      if debug then console.log 'spinredis on message got ' + replystr

      reply = JSON.parse(replystr)
      status = reply.status
      message = reply.payload
      info = reply.info

      if info == 'list of available targets'
        console.log 'Spincycle server channel is up and awake'
        @open = true
      else
        if message and message.error and message.error == 'ERRCHILLMAN'
          oldmsg = @savedMessagesInCaseOfRetries[reply.messageId]
          if oldmsg
            console.log 'got ERRCHILLMAN from spinycle service, preparing to retry sending message...'
            setTimeout(
              ()=>
                console.log 'resending message '+oldmsg.messageId+' due to target endpoint not open yet'
                @emit(oldmsg)
              ,250
            )

        else if not @hasSeenThisMessage reply.messageId
          @savedMessagesInCaseOfRetries.remove(reply.messageId)
          if reply.messageId and reply.messageId isnt 'undefined' then @seenMessages.push(reply.messageId)
          if @seenMessages.length > 10 then @seenMessages.shift()
          if debug then console.log 'redis-spincycle got reply messageId ' + reply.messageId + ' status ' + status + ', info ' + info + ' data ' + message + ' outstandingMessages = ' + @outstandingMessages.length
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
            subs = @subscribers[info]
            if subs
              subs.forEach (listener) ->
                listener message
            else
              if debug then console.log 'no subscribers for message ' + message
              if debug then console.dir reply
        else
          if debug then console.log '-- skipped resent message ' + reply.messageId

  hasSeenThisMessage: (messageId) =>
    @seenMessages.some (mid) -> messageId == mid

  registerListener: (detail) =>
    #console.log 'spinclient::registerListener called for ' + detail.message
    subs = @subscribers[detail.message] or []
    subs.push detail.callback
    @subscribers[detail.message] = subs

  registerObjectSubscriber: (detail) =>
    d = $q.defer()
    sid = uuid.v4()
    localsubs = @objectsSubscribedTo[detail.id]
    #console.log 'register@objectsSubscriber localsubs is'
    #console.dir localsubs
    if not localsubs
      localsubs = []
      console.log 'spinredis no local subs, so get the original server-side subscription for id ' + detail.id
      # actually set up subscription, once for each @objects
      @_registerObjectSubscriber({
        id: detail.id, type: detail.type, cb: (updatedobj) =>
          #console.log '-- register@objectsSubscriber getting obj update callback for ' + detail.id
          lsubs = @objectsSubscribedTo[detail.id]
          #console.dir(lsubs)
          for k,v of lsubs
            if (v.cb)
              #console.log '--*****--*****-- calling back @objects update to local sid --****--*****-- ' + k
              v.cb updatedobj
      }).then( (remotesid) =>
          localsubs['remotesid'] = remotesid
          localsubs[sid] = detail
          #console.log '-- adding local callback listener to @objects updates for ' + detail.id + ' local sid = ' + sid + ' remotesid = ' + remotesid
          @objectsSubscribedTo[detail.id] = localsubs
          d.resolve(sid)
        ,(rejection)=>
          console.log 'spinredis registerObjectSubscriber rejection: '+rejection
          console.dir rejection
      )
    else
      localsubs[sid] = detail
    return d.promise

  _registerObjectSubscriber: (detail) =>
    d = $q.defer()
    #console.log 'spinredis message-router registering subscriber for @objects ' + detail.id + ' type ' + detail.type
    subs = @objsubscribers[detail.id] or []

    @emitMessage({target: 'registerForUpdatesOn', obj: {id: detail.id, type: detail.type}}).then(
      (reply)=>
        console.log 'spinredis server subscription id for id ' + detail.id + ' is ' + reply
        subs[reply] = detail.cb
        @objsubscribers[detail.id] = subs
        d.resolve(reply)
    , (reply)=>
      @failed(reply)
    )
    return d.promise

  deRegisterObjectsSubscriber: (sid, o) =>
    localsubs = @objectsSubscribedTo[o.id] or []
    if localsubs[sid]
      console.log 'deregistering local updates for @objects ' + o.id
      delete localsubs[sid]
      count = 0
      for k,v in localsubs
        count++
      if count == 1 # only remotesid property left
        @_deRegisterObjectsSubscriber('remotesid', o)

  _deRegisterObjectsSubscriber: (sid, o) =>
    subs = @objsubscribers[o.id] or []
    if subs and subs[sid]
      delete subs[sid]
      @objsubscribers[o.id] = subs
      @emitMessage({target: 'deRegisterForUpdatesOn', id: o.id, type: o.type, listenerid: sid}).then (reply)->
        console.log 'deregistering server updates for @objects ' + o.id

  emitMessage: (detail) =>
    if debug then console.log 'emitMessage called'
    if debug then console.dir detail
    d = $q.defer()
    detail.messageId = uuid.v4()
    detail.sessionId = detail.sessionId or @sessionId
    detail.d = d
    @outstandingMessages.push detail
    if debug then console.log 'saving outstanding reply to messageId ' + detail.messageId + ' and @sessionId ' + detail.sessionId
    @emit detail

    return d.promise

# ------------------------------------------------------------------------------------------------------------------

  getModelFor: (type) =>
    d = $q.defer()
    if @modelcache[type]
      d.resolve(@modelcache[type])
    else
      @emitMessage({target: 'getModelFor', modelname: type}).then((model)->
          @modelcache[type] = model
          d.resolve(model)
        ,(rejection)=>
          console.log 'spinredis getModelFor rejection: '+rejection
          console.dir rejection
      )
    return d.promise

  listTargets: () =>
    d = $q.defer()
    @emitMessage({target: 'listcommands'}).then((targets)->
        d.resolve(targets)
      ,(rejection)->
        console.log 'spinredis listTargets rejection: '+rejection
        console.dir rejection
    )
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
