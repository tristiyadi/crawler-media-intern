const Schedule = require('node-schedule');
const Fs = require('fs');
const Crypto = require("crypto");
const Moment = require('moment');
Moment.locale('id');
const Dotenv = require('dotenv');
Dotenv.config();
const Axios = require('axios').default;
const Natural = require('natural');
const { Fds } = require("./connections");

let Sources = require("./data/sources.json")
let Download =  require('./includes/Download.js');
let LinkScanner = require('./includes/LinkScanner.js');
let DataManager = require('./includes/DataManager.js');
let ProcessLink = require('./includes/ProcessLink.js');
const SentimentAnalysis = require("./includes/AnalyzeModule/SentimentModule/SentimentAnalysis.js");

const UV_THREADPOOL_SIZE = process.env.UV_THREADPOOL_SIZE;
const REDIS_PORT = process.env.REDIS_PORT;
//Ende imports

console.log(' =============================================( STARTING CRAWLING... )=========================================');

try {
    (async () => {
        let dm = await new DataManager(REDIS_PORT, async function(){
            let downloadLinks = await function(){
                //DownloadLinks, send to scanner
                for (i = 0; i < Sources.length; i++){
                    console.log("Download: " + JSON.stringify(Sources[i]));
                    new Download(s, Sources[i].id, Sources[i].url);
                }
            };

            let processLinks = await function(){
                dm.getArticleToProcess(function(link, id){
                    console.log("Process: " + id, "Link: "+link);
                    ProcessLink.processLink(link, id, dm);
                    processLinks();      
                });
            };
            
            //LinkScanner for Download
            let s = await  new LinkScanner( async function(sourceId, links){
                await dm.saveCurrentScan(sourceId, links);
            });

            // Schedule.scheduleJob('*/10 * * * *', downloadLinks);
            // Schedule.scheduleJob('5-59/10 * * * *', processLinks);

            downloadLinks();
            setTimeout(processLinks, 1000 * 10);
            
            //dm.disconnect();
        });
})()
} catch (err) {
    console.error(err)
}
