const config = require('./config').rados;
const rados = require('rados');
const hs = require('hyperscan');
const io = require('io');
const pool = require('fib-pool');
const zlib = require('zlib');

let eolReg = hs.compile("\n", "L");

let cluster = new rados.Rados(config.clustername, config.username, config.conf);
let radosPool = pool(() => {
    cluster.connect();
    return cluster.createIoCtx('logs')
})

function splitTime(timeRange) {
    return ['2017-08-28/04:28']
}

function search(servers, timeRange, content) {
    if (!servers || servers.length === 0) {
        let errMsg = "search: servers is null"
        console.error(errMsg)
        return {
            error: errMsg
        }
    }
    if (!timeRange || timeRange.length === 0) {
        let errMsg = "search: timeRange is null"
        console.error(errMsg)
        return {
            error: errMsg
        }
    }
    if (!content) {
        let errMsg = "search: content is null"
        console.error(errMsg)
        return {
            error: errMsg
        }
    }
    let ret = [];
    let hsReg = hs.compile(content, "L");
    timeRange = splitTime(timeRange);
    servers.forEach(svr => {
        timeRange.forEach((time) => {
            let stream = radosPool((conn) => {
                conn.open(svr + '/' + time)
            });
            let buf, ms, uzs, left, eolRes, pos = 0;
            ms = new io.MemoryStream();
            uzs = zlib.createGunzip(ms);
            while (stream.copyTo(uzs, 1000000) !== 0) {
                ms.seek(pos, fs.SEEK_SET);
                buf = ms.read();
                pos = ms.tell();
                if (left)
                    buf = Buffer.concat([left, buf]);

                eolRes = eolReg.scan(buf);
                if (!eolRes) {
                    left = buf;
                    continue;
                }
                let len = eolRes['\n'].length;
                let lid = eolRes['\n'][len - 1][1];
                left = buf.slice(lid, buf.length);
                buf = buf.slice(0, lid);
                let hsRes = hsReg.scan(buf);
                if (!hsRes) continue;
                let lline = 0;
                hsRes[content].forEach((pair) => {
                    for(var i = lline; i < eolRes['\n'].length; i++) {
                        let eolPair = eolRes['\n'][i];
                        
                        if(pair[1] < eolPair[1]) {
                            lline  = i;
                            if(lline === 0) 
                                ret.push(buf.slice(0, eolPair[0]).toString())
                            else
                            {
                                let lastPair = eolRes['\n'][i - 1];
                                ret.push(buf.slice(lastPair[1], eolPair[0]).toString());
                            }
                            break;
                        }
                    }
                })
            }
            if(hsReg.scan(left))
                ret.push(left.toString());
        })
    });
    console.log(ret);
}

search(['124.207.253.85'], ['2017-08-28/04:28'], '117.100.187.110')