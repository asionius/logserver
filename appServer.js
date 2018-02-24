const uuid = require('uuid');
const http = require('http');
const hash = require('hash');
const fs = require('fs');
const jws = require('fib-jws');
const rados = require('./rados');
const config = require('./config');

let privPem = fs.readFile('./pem/rsa_private_key.pem');
let pubPem = fs.readFile('./pem/rsa_public_key.pem');
let sessionStorage = {};
let response = (v, data) => {
    v.response.write(typeof (data) === 'object' ? JSON.stringify(data) : data);
}
config.log.forEach((log) => {
    console.add(log);
})
let svr = new http.Server(config.server.port, [(v) => {
    let jws = v.cookies['jws'];
    v.jws = jws;
}, {
    '/_signup': (v) => {
        let form = v.json();
        let username = form.username;
        let phone = form.phone;
        let password = form.password;
        rados.setUser(username, {
            password: password,
            phone: phone
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
        if (userInfo.password === password) {
            let signature = jws.sign({
                alg: "RS512"
            }, {
                username: username
            }, privPem);
            v.response.addHeader('set-Cookie', "jws=" + signature + "; Expires=" + new Date(new Date().getTime() + 7 * 24 * 60 * 60 * 1000).toGMTString() + "; path=/; domain=" + config.server.domain);
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
        if (!v.jws || !jws.verify(v.jws, pubPem)) {
            response(v, {
                code: 3000,
                msg: "need login"
            })
            return;
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
        if (generateDownloadFile)
            fs.writeFile(path.join(__dirname, 'public/result.txt'), res);
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