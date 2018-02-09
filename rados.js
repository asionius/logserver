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
    if (timeRange.length === 1)
    {
        return [new Date(timeRange[0]).format('yyyy-MM-dd/hh:mm')];
    }
    let t, r = [];
    let s = new Date(timeRange[0]).getTime();
    let e = new Date(timeRange[1]).getTime();
    while (t = (s + 1000 * 60) <= e) {
        r.push(new Date(t).format('yyyy-MM-dd/hh:mm'))
    }
}

function search(servers, timeRange, content) {
    let ret = [];
    let hsReg = hs.compile(content, "L");
    timeRange = splitTime(timeRange);
    servers.forEach(svr => {
        timeRange.forEach((time) => {
            let stream = radosPool((conn) => {
                let key = svr + '/' + time;
                return conn.open(key);
            });
            let buf, ms, uzs, left, eolRes, pos = 0,
                lline = 0;
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
            }
            if (hsReg.scan(left)) {
                ret.push(left.toString());
            }
            ms.close();
            uzs.close();
            stream.close();
        })
    });
    return ret;
}
module.exports = {
    search: search,
    getUser: getUser,
    setUser: setUser
};