var rados = require('rados')
var zlib = require('zlib')
var io = require('io');
var cluster = new rados.Rados('ceph', 'client.admin', '/etc/ceph/ceph.conf');
cluster.connect()

console.log('connect success...');

console.log(cluster.listPool());
var ioctx = cluster.createIoCtx('test')
//var stream = ioctx.open('124.207.253.85/2017-08-28/04:28')
var stream = ioctx.open('test')
//
stream.read();
//console.log('stream.seize: ' + stream.size());
//console.time('gunzip');
//var buffer = zlib.gunzip(stream.read());
//console.timeEnd('gunzip');
//console.log(buffer.length);
//stream.rewind();
//
//var ms = new io.MemoryStream();
//var gunzipStream = zlib.createGunzip(ms);
//console.time('copyTo');
//stream.copyTo(gunzipStream);
//console.timeEnd('copyTo');
//ms.rewind();
//ms.read();
//console.log(ms.());