# Simple MQTT Server

This is simple MQTT 3.1.1 server implemented in C. This was part of recreational programming session and hence does not have full functionality as per spec.

Spec reference: http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.pdf


## How to use?

```
gcc mqtt_server.c -o mqtt_server && ./mqtt_server
```
Yup, that's it.

## What to expect when it's run?

When it's run, it starts listening on default MQTT port 1883. When a client connects to it, it'll start the interactive prompt `>>>`.
You can see the traces in the file `/tmp/mqtt_server.log`.  
The server takes care of responding to `CONNECT`, `SUBSCRIBE` and `PINGREQ` packets so that the connection is established and maintained. The interactive shell can be used to send `PUBLISH` messages. This server does not handle any other message types.

Have Fun `;)`