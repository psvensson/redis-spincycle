var expect = require("chai").expect;
var util = require('util');

var spinredis = require('../lib/spinredis')

describe("Redis spinclient", function()
{
    var sr = new spinredis();
    var result = null;

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
})
