const redis = require('redis');
const Crypto = require("crypto");
let storage = require("node-persist");
let Sources = require("./../data/sources.json")
//let Article = require('./Article.js');
let Link = require('./Link.js');
const { fds } = require("./../connections");

const SentimentAnalysis = require("./AnalyzeModule/SentimentModule/SentimentAnalysis.js");
let sentiment = new SentimentAnalysis();

var docs = [];
var config = {
    bulkEvery: 10,
    index: 'media_internasional',
    type: 'article',
};
var counter = {
    found: 0,
    indexed: 0,
    scriptStart: process.hrtime()
};
storage.init({
    dir: __dirname+'/logs/',
    stringify: JSON.stringify,
    parse: JSON.parse,
    encoding: 'utf8',
    logging: false,  // can also be custom logging function
    forgiveParseErrors: false 
});
module.exports = class DataManager{
    constructor(port, finishedInit){
        let theBase = this;
        this.client = redis.createClient(port);        
        this.client.on('connect', function() {
            console.log('Redis connected');

            theBase.client.get('lastLinkId', function(err, reply) {
                if(typeof reply == 'undefined' || reply == null){
                    theBase.client.set('lastLinkId', '0');
                    console.log("Link id init to: 0");
                    theBase.lastLinkId = 0;
                }else{
                    console.log("Link id: " + reply);
                    theBase.lastLinkId = parseInt(reply);
                }

                console.log("");
                finishedInit();
            });
        });        
    }

    saveCurrentScan(sourceId, links)
    {
        let linkUrls = [];
        let linksViaUrls = [];
        let data=[];
        for(let i in links){
            linkUrls.push(links[i].url);
            linksViaUrls[links[i].url] = links[i];
            data.push(links[i]);
        }

        let theBase = this;
        let blacklistName = 'blacklist:' + sourceId;
        let oldBlacklistName = 'oldBlacklist:' + sourceId;
        let currentScanName = 'currentScan:' + sourceId;

        theBase.client.sadd(currentScanName, linkUrls, function(err, reply) {
            //console.log("Added to " + currentScanName + ": " + reply);
            theBase.client.sdiff(currentScanName, blacklistName, oldBlacklistName, function(err, reply) {
                console.log("New for " + sourceId + ": " + reply);
                for(let i in reply){
                    let url = reply[i];
                    let link = linksViaUrls[url];
                    let id = theBase.lastLinkId++;
                    let data = link==undefined ? [] : link.getDataArray();
                    data.push('id', id);
                    theBase.client.hmset("link:" + id, data);
                    // map to bulk es
                    if(data.length>0){
                        try{
                            let doc = {};
                            doc.url = reply[i];
                            doc.link = linksViaUrls[url];
                            doc.title = data[1];
                            doc.description = data[1];
                            doc.timestamp = data[3];
                            doc.sourceId = sourceId;
                            doc.base_url=null;
                            doc.name_news=null;
                            for (let j = 0; j < Sources.length; j++){
                                if(Sources[j].id=sourceId){
                                    doc.base_url = Sources[j].url;
                                    doc.name_news = Sources[j].name;
                                    break;
                                }
                            }
                            doc.sentiment = sentiment.getSentimentScore(data[1].split(" "));

                            let timestamp = Math.floor(Date.now() / 1000)
                            let nid = Crypto.createHash("sha1").update(timestamp.toString()+String(doc.url)).digest("hex");
                            docs.push({ index: {_index: config.index, _type: config.type, _id: nid} });
                            docs.push(doc);
                        }catch(err){
                            console.error(err)
                        }
                    }
                }
                if (docs.length>0) { // && docs.length >= config.bulkEvery
                    console.log("Bulk every len ", docs.length)
                    storage.setItem('article_'+sourceId,JSON.stringify(docs))
                    .then(function() {
                        console.log("Saved Data Crawler");
                    });
                    theBase.bulkInsert(docs);
                }
                if(reply.length != 0){
                    theBase.client.set('lastLinkId', theBase.lastLinkId);
                    console.log("lastLinkId is now: " + theBase.lastLinkId);
                    theBase.client.sadd(blacklistName, reply, function(err, reply) {
                        //console.log("Added to blacklist " + reply + " | " + err);
                    });
                }

                theBase.client.del(currentScanName, function(err, reply){
                    //console.log("Deleted currentScan " + reply + " | " + err);
                });
            });
        });
    }
    
    bulkInsert(docs, done) {
        console.log('Bulk index', docs.length);
        fds.bulk({
            refresh: true,
            body: docs
        }, (err, resp) => {
            if (err) {
                console.log('bulk error', err);
                // return process.exit(1);
            }
            counter.indexed = counter.indexed + (docs.length);
            console.log('Bulk index '+config.index+' done', counter.indexed);
            if (done) {
                console.log('All done, total found documents', counter.found);
                console.log('Total indexed documents', counter.indexed);
                var elapsed = process.hrtime(counter.scriptStart);
                elapsed =  Math.round(elapsed[0]/60) +'m '+ elapsed[0] +'s '+  Math.round(elapsed[1]/1000000) +'ms';
                console.log('Time elapsed', elapsed);
                console.log(' =============================================( FINISH CRAWLING... )=========================================');
                // process.exit();
            }
        });
    }

    getArticleToProcess(callback)
    {
        let theBase = this;
        theBase.client.get('lastProcessedLinkId', function(err, reply) {
            if(typeof reply == 'undefined' || reply == null){
                theBase.client.set('lastProcessedLinkId', '0');
                console.log("lastProcessedLinkId init to: 0");
                theBase.lastProcessedLinkId = 0;
            }
            else
            {
                theBase.lastProcessedLinkId = parseInt(reply);
            }

            let nextId = theBase.lastProcessedLinkId + 1;
            theBase.client.hgetall("link:" + nextId, function(err, reply) {
                if(reply != null){
                    callback(new Link(reply.title, reply.date, reply.url, reply.sourceId), reply.id);
                    theBase.client.incr('lastProcessedLinkId');
                }
                else
                {
                    //No more links
                }
            });
        });
    }

    /*getLinks(linkArrived)
    {
        for(let i = 0; i < this.lastLinkId; i++)
        {
            this.client.hgetall("link:"+i, function(err, reply) {
                if(reply != null)
                    linkArrived(new Link(reply.title, reply.date, reply.url, reply.sourceId), reply.id);
            });
        }        
    }*/

    /*resetProcessedDataCounter()
    {
        this.client.set('lastProcessedLinkId', '0');
    }*/

    cleanSlate()
    {
        let theBase = this;
        this.client.keys('*', function (err, keys) {
            for(let i = 0, len = keys.length; i < len; i++) {
                theBase.client.del(keys[i], function(err, reply){
                    console.log("Deleted: " + keys[i] + " | " + reply + " | " + err);
                });
            }
        });
    }

    deleteRedundancies()
    {
        let theBase = this;
        this.client.keys('*', function (err, keys) {
            for(let i = 0, len = keys.length; i < len; i++) {
                if(keys[i] != "lastLinkId" && keys[i].startsWith("blacklist:") == false && keys[i].startsWith("oldBlacklist:") == false && keys[i].startsWith("link:") == false)
                {
                    //console.log("To delete: " + keys[i]);
                    theBase.client.del(keys[i], function(err, reply){
                        console.log("Deleted: " + keys[i] + " | " + reply + " | " + err);
                    });
                }
            }
        });
    }

    disconnect()
    {
        this.client.quit();
    }
}