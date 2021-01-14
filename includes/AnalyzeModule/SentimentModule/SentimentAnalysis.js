const fs = require('fs');

module.exports = class {

    constructor(){
        this.dict = {};
        function loadAnnotatedFile(path, key, value, dict){
            var array = fs.readFileSync(path).toString().split("\n");
            for(let i in array) {
                let word = array[i].toLowerCase();
                word = word.replace("\r", "");
                word = word.replace(" ", "");
                if(word.length > 2 && word.length < 25){
                    if(dict[word] == undefined){
                        dict[word] = {};
                        dict[word][key] = value;
                    }else{
                        if(dict[word][key] == undefined)
                            dict[word][key] == value;
                        else if(dict[word][key] != value)
                        {
                            // dict[word][key]="neutral";//console.log("Found ambigoues value: " + word + " -> " + dict[word][key] + " vs " + value + " (" + key + ")");
                        }
                    }
                }
                else
                {
                    // dict[word][key]="neutral";//console.log(word + " not indexed");
                }
            }
        }

        loadAnnotatedFile(__dirname + "../../../../data/positive-words.txt", "pn", "positive", this.dict);
        loadAnnotatedFile(__dirname + "../../../../data/pstv.txt", "pn", "positive", this.dict);
        loadAnnotatedFile(__dirname + "../../../../data/negative-words.txt", "pn", "positive", this.dict);
        loadAnnotatedFile(__dirname + "../../../../data/ngtv.txt", "pn", "negative", this.dict);
        loadAnnotatedFile(__dirname + "../../../../data/my-positive.txt", "pn", "positive", this.dict);
        loadAnnotatedFile(__dirname + "../../../../data/my-negative.txt", "pn", "negative", this.dict);
    }

    getSentimentScore(wordArray){
        let score = 0;
        let foundWords = 0;
        for(let i in wordArray){
            let word = wordArray[i].toLowerCase();
            if(this.dict[word] != undefined){
                //console.log(word + " " + this.dict[word]["pn"]);
                if(this.dict[word]["pn"] == "negative"){
                    score--;
                    foundWords++;
                }else if(this.dict[word]["pn"] == "positive"){
                    score++;
                    foundWords++;
                }
            }
        }
        let val = score / foundWords;
        if(foundWords == 0 && val==0.5) return "neutral";
        if(val<0.5 && val>=0) return "negative";
        if(val>0.5 && val<=1) return "positive";
    }
}