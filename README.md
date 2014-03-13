doss-nfsd
=========

NFS interface to DOSS

Usage
-----

Build with:

    mvn package

Create an exports config file somewhere listing access:

    echo '/ 127.0.0.1(ro)' > /etc/doss-exports

Run it:

    java -jar target/doss-nfsd-*-with-dependencies.jar 1234 /path/to/blob/store /etc/doss-exports

Mount it (must be NFS 4):

    mount 127.0.0.1:/ -o port=1234,vers=4,soft

You can reload the exports file by sending SIGHUP:

    pkill -HUP -f 'java -jar target/doss-nfsd.*'
