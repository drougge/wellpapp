Server part of wellpapp, a system for image tagging.

Look at example.conf, make whatever changes you feel like, but at least
change the GUID. Then make directories dump, log and (optionally) mm_cache
in basedir.

Build the server with "make". You can set CPPFLAGS, LDFLAGS etc to find
includes or whatever. Needs openssl and libbz2, bundles utf8proc.
Run with ./server config.conf

Then you need something that talks to the server.
Use [the python client](https://github.com/drougge/wellpapp-pyclient).
