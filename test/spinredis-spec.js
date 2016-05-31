var expect = require("chai").expect;
var util = require('util');
var spinexample = require('../node_modules/spincycle/example/server.js')


var spinredis = require('../lib/spinredis')

debug = process.env['DEBUG']

describe("Redis spinclient", function()
{
    var sr, ss
    var result = null;

    before(function(done)
    {
        sr = new spinredis();
        //ss = new spincycle()
        setTimeout(function()
        {
            done()
        }, 30)
    })

    it("should send a message to spincycle and get a reply back", function(done)
    {
        msg =
        {
            target: 'foo',
            obj:{id:17, type:'FOO'}
        };

        sr.emitMessage(msg).then(function(reply)
        {
          console.log('we got a reply. Yay!');
          expect(reply).to.exist
          done()
        }, function(reject)
        {
            expect(reject).to.exist
            done()
        })

    })

    it("should be able to list sample games and subscribe to changes to one of them", function(done)
    {
        sr.emitMessage({target:'_listSampleGames'}).then(function(reply)
        {
            //console.log('we got a reply. games are:');
            //console.dir(reply)
            var game = reply[0]

            var onUpdate = function(ugame)
            {
                expect(ugame.name).to.include('_changed_eh')
                done()
            }

            console.log('-- spinredis subscribing to SampleGame '+game.id)
            sr.registerObjectSubscriber({type:'SampleGame', id:game.id, cb: onUpdate}).then(function(reply2)
            {
                game.name += '_changed_eh'

                sr.emitMessage({target:'_updateSampleGame', type:'SampleGame', obj:game}).then(function(reply3)
                {
                    console.log('update successful')
                    //console.dir(reply3)
                })

            })



        }, function(reject)
        {
            expect(reject).to.exist
            done()
        })

    })

})
