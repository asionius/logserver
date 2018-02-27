const uuid = require('uuid');
const http = require('http');
const hash = require('hash');
const fs = require('fs');
const path = require('path');
const util = require('util');
const jws = require('fib-jws');
const rados = require('./rados');
const config = require('./config');

let privPem = fs.readFile('./pem/rsa_private_key.pem');
let pubPem = fs.readFile('./pem/rsa_public_key.pem');
let sessionStorage = new util.LruCache(1000000, 1000 * 60 * 60 * 24 * 7);
let response = (v, data) => {
    v.response.write(typeof (data) === 'object' ? JSON.stringify(data) : data);
}
config.log.forEach((log) => {
    console.add(log);
})

let svr = new http.Server(config.server.port, [(v) => {
    let jwt = v.cookies['jwt'];
    v.jwt = jwt;
}, {
    '/_signup': (v) => {
        let form = v.json();
        let username = form.username;
        let phone = form.phone;
        let password = form.password;
        let admin = false;
        if (username === 'administrator') {
            admin = true;
        } else {
            let jwt = v.jwt;
            let session = sessionStorage.get(jwt);
            if (!session) {
                if (!jwt || !jws.verify(jwt, pubPem)) {
                    response(v, {
                        code: 3000,
                        msg: "need login"
                    })
                    return;
                }
                let ss = jwt.split('.');
                let payload = JSON.parse(decodeURI(ss[1]));
                sessionStorage.set(jwt, payload);
            }
            if (!session.isadmin) {
                response({
                    code: 4000,
                    msg: "not permit"
                })
                return;
            }
        }
        if(rados.getUser(username)) {
            response({
                code: 5000,
                msg: "username already exists"
            })
            return;
        }
        rados.setUser(username, {
            password: hash.md5(password).digest().toString('hex'),
            phone: phone,
            isadmin: admin
        })
        response(v, {
            code: 0,
            msg: "success"
        })
    },
    '/_login': (v) => {
        let form = v.json();
        let username = form.username;
        let password = form.password;
        let userInfo = rados.getUser(username);
        if (userInfo.password === hash.md5(password).digest().toString('hex')) {
            let signature = jws.sign({
                alg: "RS512"
            }, {
                username: username,
                isadmin: userInfo.isadmin
            }, privPem);
            v.response.addHeader('set-Cookie', "jwt=" + signature + "; Expires=" + new Date(new Date().getTime() + 7 * 24 * 60 * 60 * 1000).toGMTString() + "; path=/; domain=" + config.server.domain);
            response(v, {
                code: 0,
                msg: "success"
            })

            return;
        }
        response(v, {
            code: 1000,
            msg: "password or username wrong"
        })
    },
    '/_search': (v) => {
        let jwt = v.jwt;
        let session = sessionStorage.get(jwt);
        if (!session) {
            if (!jwt || !jws.verify(jwt, pubPem)) {
                response(v, {
                    code: 3000,
                    msg: "need login"
                })
                return;
            }
            let ss = jwt.split('.');
            let payload = JSON.parse(decodeURI(ss[1]));
            sessionStorage.set(jwt, payload);
        }
        let form = v.json();
        let servers = form.servers;
        let timeRange = form.timeRange;
        let content = form.content;
        let generateDownloadFile = form.generateDownloadFile;

        if (!servers || servers.length === 0) {
            let errMsg = "search: servers is null"
            console.error(errMsg)
            response(v, {
                code: 2000,
                msg: errMsg
            });
        }
        if (!timeRange || timeRange.length === 0) {
            let errMsg = "search: timeRange is null"
            console.error(errMsg)
            response(v, {
                code: 2000,
                msg: errMsg
            });
        }
        if (!content) {
            let errMsg = "search: content is null"
            console.error(errMsg)
            response(v, {
                code: 2000,
                msg: errMsg
            });
        }
        let ret = rados.search(servers, timeRange, content);
        let res = ret.join('\n');
        if (generateDownloadFile) {
            fs.writeFile(path.join(__dirname, 'public/result.txt'), res);
            response(v, {
                code: 0
            });
            return;
        }
        response(v, {
            code: 0,
            data: res
        });
    },
    '*': (v) => [http.fileHandler('./public'),
        (v, url) => {
            if (/\/(login)|(signup)|(search).*/.test(url)) {
                v.response.addHeader('Content-Type', 'text/html');
                v.response.body = fs.openFile('./public/index.html');
                v.response.statusCode = 200;
            }
        }
    ]
}]);

console.log('server start running ...');
svr.run();