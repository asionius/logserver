const config = require('./config').rados;
const rados = require('rados');
const hs = require('hyperscan');
const fs = require('fs');
const io = require('io');
const pool = require('fib-pool');
const zlib = require('zlib');
require('./dateFormat');

let eolReg = hs.compile("\n", "L");

let cluster = new rados.Rados(config.clustername, config.username, config.conf);
cluster.connect();

let radosPool = pool(() => {
    return cluster.createIoCtx(config.poolname)
})
let authPool = pool(() => {
    return cluster.createIoCtx(config.authpool)
})

function getUser(username) {
    let stream = authPool((conn) => {
        return conn.open(username);
    })
    let buf = stream.read();
    stream.close();
    if (buf) return JSON.parse(buf.toString());
    else return null;
}

function setUser(username, userInfo) {
    let stream = authPool((conn) => {
        return conn.open(username);
    })
    stream.write(JSON.stringify(userInfo));
    stream.close();
}

function splitTime(timeRange) {
    if (timeRange.length === 1) {
        return [new Date(timeRange[0]).format('yyyy-MM-dd/hh:mm')];
    }
    let t, r = [];
    let s = new Date(timeRange[0]).getTime();
    let e = new Date(timeRange[1]).getTime();
    for (var i = 0;; i++) {
        t = s + 60000 * i;
        if (t <= e)
            r.push(new Date(t).format('yyyy-MM-dd/hh:mm'))
        else break;
    }
    return r;
}

function search(servers, timeRange, content) {
    let ret = [];
    content = content.replace(/([\?\-\*\.\/\|])/g, '\\$1');
    content = "[^\n]*" + content +".*\n";
    let hsReg = hs.compile(content, "L");
    timeRange = splitTime(timeRange);
    servers.forEach(svr => {
        timeRange.forEach((time) => {
            let stream = radosPool((conn) => {
                let key = svr + '/' + time;
                console.log(key);
                return conn.open(key);
            });
            let buf, ms, uzs, eolRes, pos = 0, len, sum = 0;
            ms = new io.MemoryStream();
            uzs = zlib.createGunzip(ms);
            // console.log(`stream size: ${stream.size()}`);
            try {
                while (stream.copyTo(uzs, 1000000) !== 0) {
                    sum += len;
                    // console.log(`sum: ${sum}`)
                    ms.seek(pos, fs.SEEK_SET);
                    buf = ms.read();
                    pos = ms.tell();
                    let hsRes = hsReg.scan(buf);
                    if (!hsRes)
                        continue;
                    hsRes[content].forEach((pair) => {
                        ret.push(buf.slice(pair[0], pair[1]).toString());
                    })
                }
            } catch (e) {
                console.error(e)
            }
            ms.close();
            uzs.close();
            stream.close();
        })
    });
    return ret;
}

function search1(servers, timeRange, content) {
    let ret = [];
    let stream, ms, bs, uzs, pos = 0, buf, data, lines, len, sum = 0;
    timeRange = splitTime(timeRange);
    for(let j in servers) {
        for(let i in timeRange) {
            sum = 0;
            pos = 0;
            stream = radosPool((conn) => {
                let key = servers[j] + '/' + timeRange[i];
                console.log(key);
                return conn.open(key);
            });
            ms = new io.MemoryStream();
            uzs = zlib.createGunzip(ms);
            console.log('size ' + stream.size());
            while ((len = stream.copyTo(uzs, 1000000)) !== 0) {
                sum += len;
                console.log('sum ' + sum)
                ms.seek(pos, fs.SEEK_SET);
                buf = ms.readAll();
                data = buf.toString();
                pos = ms.tell();
                lines = data.split('\n');
                lines.forEach((line) => {
                    if (line.indexOf(content) > -1) {
                        ret.push(line);
                    }
                })
            }
            ms.close();
            uzs.close();
            stream.close();
        }
    }
    return ret;
}
module.exports = {
    search: search,
    getUser: getUser,
    setUser: setUser
};