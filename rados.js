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
    let hsReg = hs.compile(content, "L");
    timeRange = splitTime(timeRange);
    servers.forEach(svr => {
        timeRange.forEach((time) => {
            let stream = radosPool((conn) => {
                let key = svr + '/' + time;
                return conn.open(key);
            });
            let buf, ms, uzs, left, eolRes, pos = 0;
            let cpTime = 0,
                scTime = 0,
                cpSum = 0,
                scSum = 0;
            ms = new io.MemoryStream();
            uzs = zlib.createGunzip(ms);
            let t0 = new Date().getTime();
            while (stream.copyTo(uzs, 1000000) !== 0) {
                ms.seek(pos, fs.SEEK_SET);
                buf = ms.read();
                pos = ms.tell();
                let t = new Date().getTime();
                cpTime = t - t0;
                cpSum += cpTime;
                if (left)
                    buf = Buffer.concat([left, buf]);

                eolRes = eolReg.scan(buf);
                if (!eolRes) {
                    left = buf;
                    t0 = new Date().getTime();
                    continue;
                }
                let len = eolRes['\n'].length;
                let lid = eolRes['\n'][len - 1][1];
                left = buf.slice(lid, buf.length);
                buf = buf.slice(0, lid);
                let hsRes = hsReg.scan(buf);
                if (!hsRes) {
                    t0 = new Date().getTime();
                    continue;
                }
                let lline = 0;
                hsRes[content].forEach((pair) => {
                    for (var i = lline; i < eolRes['\n'].length; i++) {
                        let eolPair = eolRes['\n'][i];

                        if (pair[1] < eolPair[1]) {
                            lline = i;
                            if (lline === 0) {
                                let rs = buf.slice(0, eolPair[0]).toString()
                                ret.push(rs)
                            } else {
                                let lastPair = eolRes['\n'][i - 1];
                                let rs = buf.slice(lastPair[1], eolPair[0]).toString()
                                ret.push(rs);
                            }
                            break;
                        }
                    }
                })
                t0 = new Date().getTime();
            }
            if (hsReg.scan(left)) {
                ret.push(left.toString());
            }
            console.log('cp time total', cpSum);
            ms.close();
            uzs.close();
            stream.close();
        })
    });
    return ret;
}

function search1(servers, timeRange, content) {
    let ret = [];
    timeRange = splitTime(timeRange);
    servers.forEach(svr => {
        timeRange.forEach((time) => {
            let stream = radosPool((conn) => {
                let key = svr + '/' + time;
                return conn.open(key);
            });
            let ms, bs, uzs, pos = 0;
            ms = new io.MemoryStream();
            bs = new io.BufferedStream(ms);
            uzs = zlib.createGunzip(ms);
            while (stream.copyTo(uzs, 1000000) !== 0) {
                ms.seek(pos, fs.SEEK_SET);
                let data = ms.readAll().toString();
                pos = ms.tell();
                let lines = data.split('\n');
                lines.forEach((line) => {
                    if (line.indexOf(content) > -1) {
                        ret.push(line);
                    }
                })
            }
            ms.close();
            uzs.close();
            stream.close();
        })
    });
    return ret;
}
module.exports = {
    search: search1,
    getUser: getUser,
    setUser: setUser
};