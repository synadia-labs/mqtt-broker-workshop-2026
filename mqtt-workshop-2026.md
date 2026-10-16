# MQTT over NATS: edge to cloud workshop

**Audience:** engineers familiar with pub/sub messaging and streaming.  
**Duration:** 1-2 hours.  
**Requirements:** macOS or Linux, sh, Internet access to `demo.nats.io` (lab 8) and several terminals (up to seven in lab 4).

Run each lab in order.  
Commands assume you are in a fresh `mqtt-nats-workshop` directory.  
This is a development lab; passwords, certificates and listeners below are for localhost only.

---

## Lab index

- [Lab 0 — Setup](#setup)
- [Lab 1 — MQTT hello world, QoS and retained state](#lab-1--mqtt-hello-world-qos-and-retained-state)
- [Lab 2 — Client interoperability MQTT <---> NATS](#lab-2--client-interoperability-mqtt-----nats)
- [Lab 3 — JetStream history versus retained message](#lab-3--jetstream-history-versus-retained-message)
- [Lab 4 — Three-server cluster](#lab-4--three-server-cluster)
- [Lab 5 — Accounts and isolation (tenancy)](#lab-5--accounts-and-isolation-tenancy)
- [Lab 6 — mTLS on the MQTT listener](#lab-6--mtls-on-the-mqtt-listener)
- [Lab 7 — Auth Callout](#lab-7--auth-callout)
- [Lab 8 — Edge to cloud: leaf link and store and forward](#lab-8--edge-to-cloud-leaf-link-and-store-and-forward)

---

## Takeaways

1. **Keep existing MQTT devices speaking what they already know.** MQTT 3.1.1 clients connect directly to the NATS MQTT listener; no device rewrite or separate bridge.
2. **One subject space.** MQTT `factory/line1/temp` becomes NATS `factory.line1.temp`; native services can use the same data.
3. **Use the native NATS API for applications.** Pub/sub, request/reply, queue groups, services and JetStream features without limiting application design to MQTT semantics.
4. **Distinguish delivery concepts.** MQTT QoS, retained last values, sessions and JetStream history solve different problems.
5. **Scale connections and routing.** Clustered servers for availability; leaf links join remote sites by interest.
6. **Keep the edge alive offline.** A local server serves devices during WAN loss; a local stream persists telemetry.
7. **Forward reliably after reconnection.** A cloud JetStream source can copy from an edge stream across a leaf link when both sides are under your control.
8. **Control access.** TLS, permissions, accounts and Auth Callout add security without changing the MQTT wire protocol.
9. **Compatibility.** NATS supports MQTT **3.1.1**, QoS **0, 1, 2**, and is a **Sparkplug B** (3.0) Aware server. It does **not** accept MQTT 5 clients. [NATS MQTT guide](https://docs.nats.io/learn/mqtt/).
10. **Synadia** extends NATS support to MQTT 5 protocol (full MQTT 3.1.1, SparkplugB and MQTT 5.0 Eclipse Paho conformance).

There's much, much more! Contact us -- <info@synadia.com> // <https://www.synadia.com/contact> -- we are the creators & maintainers of NATS.

---

## Setup

### Install tools

macOS (Homebrew):

```sh
brew update
brew install nats-server nats mosquitto jq mkcert go watch
```

macOS already ships `openssl` (LibreSSL), which is enough for the TLS lab. `go` is needed for lab 7 and `watch` for lab 4.

On Debian/Ubuntu Linux, install OS dependencies and recent NATS binaries with Go (the apt versions can lag):

```sh
sudo apt update
sudo apt install -y mosquitto-clients golang-go jq mkcert openssl ca-certificates
go install github.com/nats-io/nats-server/v2@latest
go install github.com/nats-io/natscli/nats@latest
export PATH="$(go env GOPATH)/bin:$PATH"
```

For Fedora use `sudo dnf install mosquitto golang jq openssl` and `go install filippo.io/mkcert@latest`; on other Linux distributions use equivalent packages. `go install` needs a Go version recent enough for current NATS releases; if apt has no `mkcert` package (older releases), use `go install filippo.io/mkcert@latest` or a binary from [mkcert releases](https://github.com/FiloSottile/mkcert/releases). If `go install` reports a minimum-version error, install a current Go from [go.dev/dl](https://go.dev/dl/) or obtain NATS release binaries from [nats-server](https://github.com/nats-io/nats-server/releases) and [natscli](https://github.com/nats-io/natscli/releases). Persist `PATH` in your shell profile if needed. Check `nats-server -v`, `nats --version`, `mosquitto_pub --help`, `mkcert --version` and `openssl version`. Use NATS Server **2.15** if possible, and nats-cli 0.5.0.

### Working directory

Create a workshop folder. Run all commands from it:

```sh
mkdir -p mqtt-nats-workshop/{conf,data,certs,callout}
cd mqtt-nats-workshop
```

**Terminal convention:** `T1` runs servers, `T2` runs subscribers, `T3` runs publishers. Occasionally we add a `T4`, or split one of those into two or three (T1-A, T1-B...). Stop foreground servers with Ctrl-C between labs. Open any new terminal in the same directory each time. Examples use ports 4222/1883/8222 for the single server, 4222–4224/1883–1885 for the cluster, and 8883 for MQTT over TLS (lab 6). Avoid colliding with a NATS or MQTT service already running.

**Creating files:** config files and code are created with `cat > <file> <<'EOF' ... EOF`. Paste the whole block, from `cat` to the closing `EOF`, into the terminal. The quoted `'EOF'` writes the content as is, without expanding `$` variables; the only exception is `edge.conf` in lab 8, which uses an unquoted `EOF` on purpose to fill in your id.

### Notes

- MQTT 5 is rejected by the NATS MQTT listener; an explicit `mosquitto_pub -V mqttv5` negative test can establish this.
- `demo.nats.io` (lab 8) is a shared public service; use unique subject prefixes, domains and stream names, never send private data, and don't assume persistent storage there. Remove your streams when done.
- Files under `data/` survive restarts. To reset a lab, stop the relevant servers and remove only that lab's data directory.

---

## Lab 1 — MQTT hello world, QoS and retained state

Create `conf/single.conf`. Check these references for [NATS Configuration](https://docs.nats.io/reference/config) and the [MQTT Settings](https://docs.nats.io/reference/config/mqtt/):

```sh
cat > conf/single.conf <<'EOF'
server_name: edge
port: 4222

jetstream {
  store_dir: "data/single"
  domain: EDGE-XYZ
}

mqtt {
  port: 1883
}
EOF
```

Run the lab:

```sh
# T1
nats-server -c conf/single.conf
```

```sh
# T2
mosquitto_sub -i observer -c -t 'factory/line1/#' -q 2 -v
```

Host, port and protocol version can be set explicitly; the defaults are equivalent to `mosquitto_sub -h localhost -p 1883 -V mqttv311`, which matches our server.

```sh
# T3
# try different QoS.
mosquitto_pub -t factory/line1/temp -m "hello qos0" -q 0
mosquitto_pub -t factory/line1/temp -m "hello qos1" -q 1
mosquitto_pub -t factory/line1/temp -m "hello qos2" -q 2
# test retain
mosquitto_pub -t factory/line1/status -m "online" -r
# get retained value
mosquitto_sub -t factory/line1/status -C 1 -v
```

Expected: the subscriber in T2 prints four values (the three qosx messages in the `temp` topic, plus the online `status`, which also matches `factory/line1/#`); a **new** subscriber (the one in T3) immediately sees retained online `status`. QoS is the MQTT exchange guarantee at this hop (QoS 2 does not turn a native NATS subscription into exactly-once processing).

Persistent sessions: stop the `observer` subscriber in T2 (Ctrl-C), publish a few temperatures with `-q 1`, then start it again with the **same** id and `-c`. It receives the messages published while it was away. A subscriber with a *fresh* id (even with `-c`) gets no history, only retained values:

```sh
# T3
mosquitto_pub -t factory/line1/temp -m "while observer was away 1" -q 1
mosquitto_pub -t factory/line1/temp -m "while observer was away 2" -q 1
mosquitto_pub -t factory/line1/temp -m "while observer was away 3" -q 1
```

```sh
# T2
# same id, gets the backlog...
mosquitto_sub -i observer -c -t 'factory/line1/#' -q 2 -v
```

```sh
# T2
# ...while a new id gets only the retained status
mosquitto_sub -i observer2 -c -t 'factory/line1/#' -q 2 -v
```

The [MQTT QoS, sessions and retained guide](https://docs.nats.io/learn/mqtt/qos-sessions-and-retained) details exactly what the server stores.

Go further:

- [Connect MQTT devices to NATS](https://docs.nats.io/learn/mqtt) — the full MQTT chapter, from first client to production.
- [MQTT config reference](https://docs.nats.io/reference/config/mqtt) — every knob on the listener: ack timeouts, max pending acks, session stream replicas, JetStream domain.

---

## Lab 2 — Client interoperability MQTT <---> NATS

Keep the server from lab 1.

Run a NATS protocol subscriber (you may need to reset the connection context with `nats context unselect` if you use nats-cli for other setups):

```sh
# T2
nats sub 'factory.line1.>'
```

And the MQTT protocol publisher:

```sh
# T3
mosquitto_pub -t factory/line1/temp -m "from MQTT" -q 1
```

Now reverse the flow:

```sh
# T2
mosquitto_sub -t factory/line1/temp -v
```

```sh
# T3
nats pub factory.line1.temp "from NATS"
```

Expected: messages flow both ways. MQTT `+` maps to NATS `*`, and a trailing `#` to `>`, with one subtlety: `#` matches zero or more levels, `>` one or more.
Use ordinary topic segments without dots or special characters for this lab.

NATS clients can also reach MQTT's retained messages and QoS state directly, through the streams the server uses to keep MQTT state.

For example, update a retained value:

```sh
# T3
nats pub '$MQTT.rmsgs.factory.line1.status' "online (by NATS)" --header "Nmqtt-RTopic: factory/line1/status" --header "Nmqtt-RFlags: 1"
```

Check it on the MQTT side:

```sh
# T2
mosquitto_sub -t 'factory/line1/#' -v
```

And similarly for QoS:

```sh
# T2
mosquitto_sub -i observer -c -t 'factory/line1/#' -q 1 -v
```

```sh
# T3
nats pub '$MQTT.msgs.factory.line1.temp' "qos1 from NATS" --header "Nmqtt-Pub: 1"
```

Some commands to explore more:

```sh
# T3
nats stream report
# Stream keeping retain values
nats stream info '$MQTT_rmsgs'
nats stream view '$MQTT_rmsgs'
```

Your MQTT devices just joined a much bigger toolbox:

- [Topics and subjects](https://docs.nats.io/learn/mqtt/topics-and-subjects) — exactly how topics map to subjects, and which characters don't survive the trip.
- [Request-reply](https://docs.nats.io/learn/core-nats/request-reply) — ask a question and get answers, something MQTT 3.1.1 has no word for, and MQTT 5 is only learning to say.
- [Services](https://docs.nats.io/learn/services) — turn a responder into a named, versioned, discoverable, load-balanced service in a few lines.

---

## Lab 3 — JetStream history versus retained message

With lab 1's server running, create a stream. Streams have *tons* of features (persistence, replication, retention limits and TTL, per-subject limits, filtering, subject transformations and more). Check options with `nats stream add --help`. Here we stick to the defaults:

```sh
# T3
# capture only temperature and pressure for all factory lines
nats stream add TELEMETRY --subjects 'factory.*.temp','factory.*.pressure' --defaults
```

Send some metrics from the MQTT world:

```sh
# T3
mosquitto_pub -t factory/line1/temp -m 'stored-A' -q 1
mosquitto_pub -t factory/line1/temp -m 'stored-B' -q 1
mosquitto_pub -t factory/line1/pressure -m 'stored-pressure-X' -q 1
mosquitto_pub -t factory/line2/temp -m 'stored-C' -q 1
```

View the data in the stream:

```sh
# T2
nats stream info TELEMETRY
nats stream view TELEMETRY
# Filter to show just temperatures in any factory line:
nats stream view TELEMETRY --subject="factory.*.temp"

# Subscribe to ongoing stream data, starting from the last temperature at each factory line
nats sub --stream TELEMETRY "factory.*.temp" --last-per-subject
# try sending a few more metrics in T3, and see here new data coming
```

All four messages are in the stream's history, even though no subscriber was listening when they were published.

A retained status is a last value for MQTT subscribers, while the stream records successive matching publishes.

Note: keep the `data/single` directory if you restart; deleting it resets the persisted streams in this lab.

NATS streams and consumers go much further: filtering, subject transforms, message TTL, work-queue semantics, many delivery patterns, scheduled publishing, counters and more!

Go down the rabbit hole here:

- [NATS JetStream Core Concepts](https://docs.nats.io/concepts/jetstream) — the big picture.
- [NATS JetStream Deep Dive](https://docs.nats.io/learn/jetstream) — streams, consumers and acks, step by step.
- [Key-Value Store](https://docs.nats.io/learn/key-value) — a retained message's big brother: buckets with history, watchers and per-key TTL, built on a stream.
- [Per-message TTL](https://docs.nats.io/learn/jetstream/message-ttl) — readings that expire by themselves.

---

## Lab 4 — Three-server cluster

Stop the single server; we'll build a three-node cluster.

Create `conf/c1.conf`:

```sh
cat > conf/c1.conf <<'EOF'
server_name: c1
port: 4222
cluster {
  name: EDGECLUSTER
  port: 6222
  routes: ["nats-route://localhost:6222"]
}
jetstream {
  store_dir: "data/c1"
  domain: EDGE-XYZ
}
mqtt {
  port: 1883
  stream_replicas: 3
}
EOF
```

Create `conf/c2.conf`:

```sh
cat > conf/c2.conf <<'EOF'
server_name: c2
port: 4223
cluster {
  name: EDGECLUSTER
  port: 6223
  routes: ["nats-route://localhost:6222"]
}
jetstream {
  store_dir: "data/c2"
  domain: EDGE-XYZ
}
mqtt {
  port: 1884
  stream_replicas: 3
}
EOF
```

Create `conf/c3.conf`:

```sh
cat > conf/c3.conf <<'EOF'
server_name: c3
port: 4224
cluster {
  name: EDGECLUSTER
  port: 6224
  routes: ["nats-route://localhost:6222"]
}
jetstream {
  store_dir: "data/c3"
  domain: EDGE-XYZ
}
mqtt {
  port: 1885
  stream_replicas: 3
}
EOF
```

Note that we specified `stream_replicas: 3` (R3) so MQTT state (sessions, retained messages, QoS tracking) is replicated too and survives a node loss.

Run each in its own terminal:

```sh
# T1-A
nats-server -c conf/c1.conf
# T1-B
nats-server -c conf/c2.conf
# T1-C
nats-server -c conf/c3.conf
```

Subscribe with both MQTT and NATS clients:

```sh
# T2-A
# connect to node c2
mosquitto_sub -p 1884 -t 'factory/#' -v
```

```sh
# T2-B
# connect to node c1
nats sub 'factory.>'
```

Publish on node c1:

```sh
# T3
mosquitto_pub -p 1883 -t factory/line1/temp -m "hello clustered" -q 1
```

In a separate terminal, monitor the state of the clustered streams. Note the replicas. Leaders are flagged with a `*` (e.g. `c3*`). Later in this lab, missing replicas will show a `!` (e.g. `c1!`):

```sh
# T4
watch nats --server :4222,:4223,:4224 stream report
```

Stop c1 (Ctrl-C in T1-A). The other two servers log errors and keep trying to reconnect; ignore that.

c2/c3 continue serving their clients. An MQTT client connected to c1 **does not automatically know alternative MQTT endpoints**; it would have to be reconnected explicitly to port 1884 or 1885 (our T2-A subscriber is on c2, so it is unaffected). MQTT5 server redirection would not help here (just valid for ordered reconnections). In real scenarios, DNS or a load balancer may be needed to help MQTT clients find other servers easily. NATS protocol clients don't need that: they are aware of clustering and automatically reconnect to any available node (configured or discovered), as the `nats sub` in T2-B does.

Send another message. Things should still work normally despite the node outage; try node c3:

```sh
# T3
mosquitto_pub -p 1885 -t factory/line1/temp -m "hello clustered during a node outage" -q 1
```

Check the `watch nats stream report` in T4: c1's replicas show as unavailable (`c1!`), but two of three servers still form a quorum (RAFT), so everything keeps working. A second failure loses quorum and the replicated streams stop accepting writes. An R5 setup survives two failures.

Starting the server `c1` again will take your cluster back to the sunny path: the exclamation mark at `c1!` will dissapear in the `watch` command at T4. The star (`*`) by a server, flags who is the replica leader for each stream.

What just kept your data safe:

- [Your first cluster](https://docs.nats.io/learn/topologies/your-first-cluster) — routes, gossip, and clients that survive a server loss.
- [Replication and R=3](https://docs.nats.io/learn/clustering/replication-and-r3) — what a quorum commit does, and doesn't, guarantee.
- [MQTT auth and clustering](https://docs.nats.io/learn/mqtt/auth-and-clustering) — run MQTT across a cluster; [`stream_replicas`](https://docs.nats.io/reference/config/mqtt/stream_replicas) sets the replicas for the MQTT session state.

---

## Lab 5 — Accounts and isolation (tenancy)

Stop the cluster servers. Create `conf/tenants.conf`:

```sh
cat > conf/tenants.conf <<'EOF'
server_name: tenants
port: 4222

jetstream {
  store_dir: "data/tenants"
  domain: EDGE-XYZ
}

mqtt {
  port: 1883
}

accounts {
  A {
    jetstream: true,
    users: [
      { user: alice, password: alice-lab }
    ] }
  B {
    jetstream: true,
    users: [
      { user: bob, password: bob-lab }
      { user: bob2, password: bob2-lab, permissions: { publish: ["factory.line1.*"], subscribe: ["factory.line1.*"] } }
    ] }
}
EOF
```

Start server:

```sh
# T1
nats-server -c conf/tenants.conf
```

Subscribe as Bob:

```sh
# T2
mosquitto_sub -u bob -P bob-lab -t factory/line1/+ -v
```

Publish as Alice:

```sh
# T3
mosquitto_pub -u alice -P alice-lab -t factory/line1/temp -m "hello from alice"
```

Bob sees nothing: Alice lives in account A. Another client in Bob's account gets through:

```sh
# T3
mosquitto_pub -u bob2 -P bob2-lab -t factory/line1/temp -m "hello from bob2 (temp)"
mosquitto_pub -u bob2 -P bob2-lab -t factory/line1/pressure -m "hello from bob2 (pressure)"
```

Sharing across accounts is also possible with exports/imports: see [cross-account](https://docs.nats.io/learn/security/cross-account).

Permissions can also allow or deny publish and subscribe per subject. In this case we restricted bob2 to publish and subscribe only in `factory/line1/+`

```sh
# T3
# this will work
mosquitto_pub -u bob2 -P bob2-lab -t factory/line1/temp -m "hello again from bob2"
# these will fail
mosquitto_pub -u bob2 -P bob2-lab -t factory/line2/temp -m "hello from bob2 that will fail"
mosquitto_sub -u bob2 -P bob2-lab -t factory/line2/+ -v
```

The denied publish is silently dropped (MQTT 3.1.1 has no way to report it to the publisher); the server log shows `Publish Violation`. The denied subscription prints `All subscription requests were denied.` and the server logs `Subscription Violation`.

Multi-tenancy, one server:

- [Accounts and multitenancy](https://docs.nats.io/learn/security/accounts-and-multitenancy) — how an account isolates a tenant's subject space.
- [Authorization](https://docs.nats.io/learn/security/authorization) — allow and deny lists, and why one allow closes everything else.
- [MQTT auth and clustering](https://docs.nats.io/learn/mqtt/auth-and-clustering) — lock a user to MQTT-only connections.

---

## Lab 6 — mTLS on the MQTT listener

Stop the previous server. This is a repeat of lab 5 (accounts and isolation), but MQTT clients now authenticate with **client certificates** instead of passwords: the server checks the certificate against the lab CA and maps its identity (the email SAN) to a configured user (`verify_and_map`).

Generate a lab CA, a server certificate with a localhost SAN, and one client cert per user:

```sh
# Create/Install the workshop's local CA (mkcert -install may ask for your password)
mkcert -install

# Copy the CA certificate (not the key) next to the configs. Location: 'mkcert -CAROOT'
cp "$(mkcert -CAROOT)/rootCA.pem" certs/

# Create server certificate
mkcert -cert-file certs/server-cert.pem -key-file certs/server-key.pem localhost 127.0.0.1 ::1

# Create client certificates; the email becomes the identity NATS maps to a user
mkcert -client -cert-file certs/alice-cert.pem -key-file certs/alice-key.pem alice@here.com
mkcert -client -cert-file certs/bob-cert.pem -key-file certs/bob-key.pem bob@there.com

# Optional: check the chain and the client identity
openssl verify -CAfile certs/rootCA.pem certs/server-cert.pem certs/alice-cert.pem certs/bob-cert.pem
openssl x509 -in certs/bob-cert.pem -noout -email
```

Create `conf/mtls.conf`:

```sh
cat > conf/mtls.conf <<'EOF'
server_name: mtls
port: 4222

# native NATS clients: same CA, same cert-to-user mapping
tls {
  cert_file: "certs/server-cert.pem"
  key_file: "certs/server-key.pem"
  ca_file: "certs/rootCA.pem"
  verify_and_map: true
}

jetstream {
  store_dir: "data/mtls"
  domain: EDGE-XYZ
}

mqtt {
  port: 8883
  tls {
    cert_file: "certs/server-cert.pem"
    key_file: "certs/server-key.pem"
    ca_file: "certs/rootCA.pem"
    verify_and_map: true
    timeout: 2
  }
}

accounts {
  include mtls-users.conf
}
EOF
```

Create `conf/mtls-users.conf` (this time we place users in a separate include file; the path is relative to `conf/mtls.conf`). Users have no password: the user name must match the certificate's email SAN:

```sh
cat > conf/mtls-users.conf <<'EOF'
A {
  jetstream: true
  users: [
    { user: "alice@here.com" }
  ]
}
B {
  jetstream: true
  users: [
    { user: "bob@there.com" }
  ]
}
EOF
```

Start server:

```sh
# T1
nats-server -c conf/mtls.conf
```

Subscribe as Bob. With `--cafile` mosquitto uses TLS and defaults to port 8883:

```sh
# T2
mosquitto_sub --cafile certs/rootCA.pem --cert certs/bob-cert.pem --key certs/bob-key.pem -t 'factory/line1/+' -v
```

Publish as Alice:

```sh
# T3
mosquitto_pub --cafile certs/rootCA.pem --cert certs/alice-cert.pem --key certs/alice-key.pem -t factory/line1/temp -m "hello from alice" -q 1
```

Bob sees nothing from Alice (account A). But another Bob client, from MQTT or native NATS, will indeed work:

```sh
# T3
mosquitto_pub --cafile certs/rootCA.pem --cert certs/bob-cert.pem --key certs/bob-key.pem -t factory/line1/temp -m "hello from another bob" -q 1

nats -s tls://localhost:4222 --tlsca certs/rootCA.pem --tlscert certs/bob-cert.pem --tlskey certs/bob-key.pem pub factory.line1.temp "hello from NATS bob"
nats -s tls://localhost:4222 --tlsca certs/rootCA.pem --tlscert certs/bob-cert.pem --tlskey certs/bob-key.pem account info
```

`account info` shows `User: bob@there.com` and `Account: B`.

These attempts fail:

```sh
# T3
# no client certificate: the TLS handshake is rejected ("client didn't provide a certificate")
mosquitto_pub --cafile certs/rootCA.pem -t factory/line1/temp -m "nope"

# no --cafile: mosquitto doesn't enable TLS and speaks plain MQTT to a TLS port; the server drops it after the 2s TLS timeout
mosquitto_pub -p 8883 --cert certs/bob-cert.pem --key certs/bob-key.pem -t factory/line1/temp -m nope

# Bob's certificate claiming to be Alice: "not authorised", the identity comes from the cert
mosquitto_pub --cafile certs/rootCA.pem --cert certs/bob-cert.pem --key certs/bob-key.pem -u alice@here.com -P x -t factory/line1/temp -m nope
```

Run `mkcert -uninstall` after the workshop to remove the CA from your trust stores.

Harden it further:

- [Encryption & TLS](https://docs.nats.io/learn/security/encryption) — TLS and mTLS on every link, plus encryption of the JetStream store on disk.
- [`mqtt.tls` reference](https://docs.nats.io/reference/config/mqtt/tls) — [`verify_and_map`](https://docs.nats.io/reference/config/mqtt/tls/verify_and_map) rules, certificate pinning, minimum TLS version, and [OCSP](https://docs.nats.io/reference/config/mqtt/tls/ocsp_peer) to reject revoked device certificates.
- [mkcert](https://github.com/FiloSottile/mkcert) — the zero-config local CA used here.

---

## Lab 7 — Auth Callout

Stop the previous server. In labs 5 and 6 users and permissions were written into the server config.

That doesn't scale for many deployments: too many clients to list, credentials that shouldn't sit in a config file. MQTT fleets at the edge hit this quickly.

[Decentralized auth](https://docs.nats.io/learn/security/decentralized-auth) removes client credentials from the server entirely (zero-trust), but each device must carry and present a JWT, which not every MQTT device handles well.

With [Auth Callout](https://docs.nats.io/learn/security/auth-callout) the server delegates authentication and authorization to a service you write. For each connection attempt it sends a signed request (username, password, client info, TLS certificate...), and the service answers with a signed user JWT (target account, permissions...) or an error. The JWT never leaves the server. Plug in any identity source (an OIDC provider such as Auth0, a database, a device onboarding system) and manage millions of identities without touching the NATS config.

For this lab, our callout service applies these silly authn/authz rules, good enough for didactic purposes:

- the password must be equal to the username, otherwise the connection is rejected
- odd username length goes to account `A`, even length to account `B`
- the user can publish only on `factory.<username>.*` (MQTT `factory/<username>/+`), and subscribe on `factory/#`

This lab needs Go (see [Setup](#setup)).

First, generate the account key pair that signs the user JWTs, and write its secret seed to a file. The callout service needs the seed to sign its responses; the server only needs the public key, to verify that the returned JWTs come from the service.

```sh
nats auth nkey gen account --output conf/callout-issuer.nk
# show the public key:
nats auth nkey show conf/callout-issuer.nk
```

Create `conf/callout.conf`. `$CALLOUT_ISSUER` is left as is in the file (quoted `'EOF'`) and the server reads it from the environment at startup, so there is no need to paste the public key:

```sh
cat > conf/callout.conf <<'EOF'
server_name: callout
port: 4222

jetstream {
  store_dir: "data/callout"
  domain: EDGE-XYZ
}

mqtt {
  port: 1883
}

accounts {
  AUTH {
    users: [
      { user: auth, password: auth-lab }
    ]
  }
  A { jetstream: true }
  B { jetstream: true }
  SYS {}
}
system_account: SYS

authorization {
  auth_callout {
    issuer: $CALLOUT_ISSUER
    account: AUTH
    auth_users: [ auth ]
  }
}
EOF
```

The callout service lives in its own AUTH account, isolated from the regular users that land in A and B. `auth` is the only user defined statically: it's the identity of the callout service itself, listed in `auth_users` so it bypasses the callout. Everyone else is decided by the service.

Create the service `callout/main.go`:

```sh
cat > callout/main.go <<'EOF'
// Auth Callout service for the MQTT over NATS workshop.
//
// Rules:
//   - accept client only if password == username
//   - odd username length -> account A, even -> account B
//   - publish allowed only on factory.<username>.*, subscribe on factory.>
package main

import (
  "log"
  "os"
  "os/signal"
  "strings"
  "syscall"

  "github.com/nats-io/jwt/v2"
  "github.com/nats-io/nats.go"
  "github.com/nats-io/nats.go/micro"
  "github.com/nats-io/nkeys"
)

func main() {
  // Account key that signs the user JWTs; its public key is the `issuer` in callout.conf.
  seed, err := os.ReadFile("../conf/callout-issuer.nk")
  if err != nil {
    log.Fatal(err)
  }
  issuer, err := nkeys.FromSeed([]byte(strings.TrimSpace(string(seed))))
  if err != nil {
    log.Fatal(err)
  }

  nc, err := nats.Connect("nats://localhost:4222", nats.UserInfo("auth", "auth-lab"))
  if err != nil {
    log.Fatal(err)
  }
  defer nc.Drain()

  svc, err := micro.AddService(nc, micro.Config{
    Name:        "auth-callout",
    Version:     "1.0.0",
    Description: "Auth callout for the MQTT over NATS workshop",
  })
  if err != nil {
    log.Fatal(err)
  }
  defer svc.Stop()

  err = svc.AddEndpoint("authorize", micro.HandlerFunc(func(r micro.Request) {
    req, err := jwt.DecodeAuthorizationRequestClaims(string(r.Data()))
    if err != nil {
      log.Println("bad request:", err)
      r.Error("400", "bad request", nil)
      return
    }
    // Debug: dump every field the server sent us. Note this includes the password.
    //if dump, err := json.MarshalIndent(req, "", "  "); err == nil {
    //  log.Printf("authorization request:\n%s", dump)
    //}

    user := req.ConnectOptions.Username
    pass := req.ConnectOptions.Password

    // Build the response, addressed to the user nkey and server that asked.
    res := jwt.NewAuthorizationResponseClaims(req.UserNkey)
    res.Audience = req.Server.ID

    switch {
    case user == "" || strings.ContainsAny(user, ".*> "):
      res.Error = "invalid username"
    case user != pass:
      res.Error = "invalid credentials"
    default:
      account := "B"
      if len(user)%2 == 1 {
        account = "A"
      }
      uc := jwt.NewUserClaims(req.UserNkey)
      uc.Name = user
      uc.Audience = account // target account (config mode: account name)
      uc.Pub.Allow.Add("factory." + user + ".*")
      // MQTT 'factory/#' subscribes to both 'factory' and 'factory.>'
      uc.Sub.Allow.Add("factory", "factory.>")
      if res.Jwt, err = uc.Encode(issuer); err != nil {
        res.Error = err.Error()
      } else {
        log.Printf("allow %q -> account %s", user, account)
      }
    }
    if res.Error != "" {
      log.Printf("deny %q: %s", user, res.Error)
    }

    token, err := res.Encode(issuer)
    if err != nil {
      log.Println("encode response:", err)
      r.Error("500", "encode response", nil)
      return
    }
    r.Respond([]byte(token))
  }), micro.WithEndpointSubject("$SYS.REQ.USER.AUTH"))
  if err != nil {
    log.Fatal(err)
  }
  log.Println("auth callout service ready")

  sig := make(chan os.Signal, 1)
  signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
  <-sig
}
EOF
```

In this example, the username becomes a subject token in permissions, so names with `.`, `*`, `>` or spaces are rejected.

Start server and service:

```sh
# T1-A
export CALLOUT_ISSUER=$(nats auth nkey show conf/callout-issuer.nk)
nats-server -c conf/callout.conf
```

```sh
# T1-B
cd callout
go mod init callout  # skip if you've done it before
go mod tidy
go run .
```

Subscribe as `alice` (5 chars, account A) and `anna` (4 chars, account B):

```sh
# T2-A
mosquitto_sub -u alice -P alice -t 'factory/#' -v
```

```sh
# T2-B
mosquitto_sub -u anna -P anna -t 'factory/#' -v
```

Publish:

```sh
# T3
# allowed: own subtree
mosquitto_pub -u alice -P alice -t factory/alice/temp -m "hello from alice"
mosquitto_pub -u anna -P anna -t factory/anna/temp -m "hello from anna"
# bob: 3 chars, account A, same as alice
mosquitto_pub -u bob -P bob -t factory/bob/temp -m "hello from bob" -q 1

# denied: not their subtree, or too deep for '*'
mosquitto_pub -u alice -P alice -t factory/anna/temp -m "alice pretending to be anna"
mosquitto_pub -u alice -P alice -t factory/alice/line1/temp -m "too deep"
# rejected: password != username
mosquitto_pub -u anna -P wrong -t factory/anna/temp -m "wrong password"
```

Expected:

- alice's subscriber sees alice's and bob's messages; anna's sees only hers (accounts A and B are isolated, as in lab 5).
- The denied publishes are silently dropped (MQTT 3.1.1 has no way to report them to the publisher); the server log shows `Publish Violation`.
- The wrong password fails with `Connection Refused: not authorised`; the service log shows `deny "anna": invalid credentials`.

NATS clients go through the same callout:

```sh
# T3
nats --user alice --password alice pub factory.alice.temp "from NATS"
nats --user alice --password alice pub factory.anna.temp "from NATS"   # Permissions Violation
```

The callout can use anything to decide: a database, LDAP/OIDC, a token in the password field, client certificates or the client IP. The request can also be encrypted (`xkey`), and in operator (JWT) mode the same mechanism works per account. See [NATS by Example Auth Callout](https://natsbyexample.com/examples/auth/callout/cli) and the [synadia-io/callout.go](https://github.com/synadia-io/callout.go) helper library.

**Client certificates:** if the client presents a certificate, as in lab 6, the callout receives it too. The certificate chain arrives as PEM in the request's `client_tls` field (`req.TLS.VerifiedChains` in Go), so the debug block above prints it (uncomment it and add `"encoding/json"` to the imports). The listener has to ask for it: give the `mqtt` block a `tls` section with `cert_file`, `key_file`, `ca_file` and `verify: true`, as in lab 6. The server can't make certificates optional, so with `verify: true` every client on that listener must present one.

Our example callout is built with the NATS services framework, so it's discoverable, observable and scalable out of the box:

```sh
# T4
nats --user auth --password auth-lab service ls
nats --user auth --password auth-lab service info auth-callout
nats --user auth --password auth-lab service stats auth-callout
```

Try running a second instance:

```sh
# T1-C
cd callout
go run .
```

Monitor stats again, and note the 2nd instance is there too:

```sh
# T4
nats --user auth --password auth-lab service stats auth-callout
```

Create a few more mqtt/nats connections with your clients in T2/T3, then monitor `service stats` again: the 2 auth-callout instances share the load!

Plug in your own identity system:

- [`auth_callout` reference](https://docs.nats.io/reference/config/authorization/auth_callout) — every option, including the `xkey` that encrypts requests.
- [Decentralized auth](https://docs.nats.io/learn/security/decentralized-auth) — the zero-trust alternative: the operator/account/user JWT chain, scoped signing keys and revocation, with no users in the server config. Hands-on in [operator mode](https://docs.nats.io/learn/security/operator-mode).
- [Decentralized auth callout](https://natsbyexample.com/examples/auth/callout-decentralized/cli) — the same pattern in operator (JWT) mode.
- [Scaling services](https://docs.nats.io/learn/services/scaling) — run N callout instances behind the built-in queue group.

---

## Lab 8 — Edge to cloud: leaf link and store and forward

Stop the previous server and service. Now the **edge** is a local server with the MQTT listener (the factory), and the **cloud** is the public `demo.nats.io` cluster. The edge connects to the cloud as a [leaf node](https://docs.nats.io/learn/topologies/leaf-nodes): messages only cross the link if there is interest on the other side, and the edge keeps working if the link goes down. `demo.nats.io` accepts leaf connections on port 7422 with no credentials.

`demo.nats.io` is **shared by everybody** in the workshop (and the Internet). Pick a short unique ID and use it for your subjects and local JetStream domain, so you don't collide with other attendees. Never send credentials or private data to it. Export it in **every terminal** you use in this lab:

```sh
# IMPORTANT!!!  
# letters and digits only
# make it unique
# Run in all your terminals (T1, T2, T3...)
export WS=your_unique_name_42_please_change
```

Create `conf/edge.conf` (it fills in your ID):

```sh
[ -n "${WS:-}" ] || { echo "ERROR: WS must be defined and unique for you" >&2; false; } && \
cat > conf/edge.conf <<EOF
server_name: edge-$WS
port: 4222
http_port: 8222
pid_file: "data/edge.pid"

jetstream {
  store_dir: "data/edge"
  domain: EDGE-$WS
}

mqtt {
  port: 1883
}

leafnodes {
  remotes: [
    { url: "nats-leaf://demo.nats.io:7422", disabled: false }
  ]
}
EOF
```

The edge (*your* edge) has its own JetStream **domain** (`EDGE-yourname42`), so its streams stay separate from the cloud, though they can be addressed explicitly from the cloud. The domain must be unique, as all the attendees' edges join the same cloud account. If it isn't, the cloud side can't disambiguate, so your request may end up answered by whichever leaf answers first (leaf permissions can prevent this, but `demo.nats.io` is configured permission-less, wild west for everybody!)

Start the edge:

```sh
# T1
nats-server -c conf/edge.conf
```

The log shows `Leafnode connection created` and `JetStream using domains: local "EDGE-yourname42", remote ""` (the demo cluster uses the default, empty, domain). Check the link in the monitoring endpoint:

```sh
# T3
curl -s localhost:8222/leafz
```

### Live traffic across the leaf

Subscribe in the cloud:

```sh
# T2
[ -n "${WS:-}" ] || { echo "ERROR: WS must be defined and unique for you" >&2; false; } && \
nats --server demo.nats.io sub "$WS.factory.>"
```

Publish from an MQTT device at the edge:

```sh
# T3
[ -n "${WS:-}" ] || { echo "ERROR: WS must be defined and unique for you" >&2; false; } && \
mosquitto_pub -t "$WS/factory/line1/temp" -m "across leaf" -q 1
```

Expected: the cloud subscriber receives `across leaf`. Reverse traffic is also possible.

### Store and forward

Create a stream at the **edge** that captures the temperatures locally:

```sh
# T3
nats stream add EDGE_TELEMETRY --subjects "factory.*.temp" --defaults
```

All attendees share one cloud stream, `WORKSHOP_TELEMETRY`, and each adds their edge as a source. Nobody publishes to it directly. It still needs a placeholder subject (its own name): the server rejects a stream with neither subjects nor sources, which is where it ends up when the last attendee removes theirs. A one-day `max-age` keeps the shared server clean. Everyone runs the same command:

```sh
# T3
nats --server demo.nats.io stream add WORKSHOP_TELEMETRY --subjects="WORKSHOP_TELEMETRY" --storage=file --retention=limits --replicas=1 --max-age=1d --defaults
```

The first one to run it wins. Creation is idempotent, but once somebody has added a source, the stream config differs from this command, and everyone else gets `stream name already in use with a different configuration (10058)`: that's fine, the stream is there; carry on.

Now add your edge as a source. A source in another JetStream domain is given as the API prefix of that domain, `$JS.<domain>.API`; with your domain that's `$JS.EDGE-yourname42.API`. The CLI can't edit sources directly, so read the current config, append your source with `jq` (keeping everyone else's), and write it back:

```sh
# T3
[ -n "${WS:-}" ] || { echo "ERROR: WS must be defined and unique for you" >&2; false; } && \
nats --server demo.nats.io stream info WORKSHOP_TELEMETRY --json \
  | jq --arg api "\$JS.EDGE-$WS.API" --arg ws "$WS" '.config
      | .sources = ([.sources[]? | select(.external.api != $api)]
                    + [{"name": "EDGE_TELEMETRY", "external": {"api": $api}, "subject_transforms": [{"src": "factory.>", "dest": ($ws + ".factory.>")}]}])' \
  > conf/workshop-telemetry-edit.json && \
nats --server demo.nats.io stream edit WORKSHOP_TELEMETRY --config conf/workshop-telemetry-edit.json -f
```

The cloud stream will eventually source from all the participants and merge all the edge feeds in the stream. All the sources are called `EDGE_TELEMETRY`; they are told apart by their API prefix, i.e. by the edge **domain**. That's why **every attendee must use a unique `WS`**: two edges with the same domain answer the same JetStream API requests, and the cloud stream would read from whichever answers first (both, or neither, reliably).

All edge devices publish to topics like `factory/line1/temp` (or line2, etc.). The different sources merged in the cloud stream include a subject transform to disambiguate that too: `factory.>` ---> `yourname42.factory.>`. This way the merged event stream preserves the origin for each event!

Check your source is listed and active (`Last Seen` gets a value, no error):

```sh
# T3
nats --server demo.nats.io stream info WORKSHOP_TELEMETRY
```

The edit command above is read-modify-write: if a neighbour edited the stream at the same moment, one of the two sources may be lost. If yours is missing from the list, run the two commands above again (it is safe to repeat; it never adds your source twice).

Publish baseline readings from the edge:

```sh
# T3
[ -n "${WS:-}" ] || { echo "ERROR: WS must be defined and unique for you" >&2; false; } && {
  mosquitto_pub -t "factory/line1/temp" -m "online data bit 1 from $WS" -q 1 &&
  mosquitto_pub -t "factory/line1/temp" -m "online data bit 2 from $WS" -q 1 &&
  mosquitto_pub -t "factory/line2/temp" -m "online data bit 3 from $WS" -q 1 &&
  mosquitto_pub -t "factory/line2/pressure" -m "data bit 4 from $WS" -q 1
}
```

Both streams (edge and cloud) have the three temperatures now. The pressure reading is not captured: the edge stream only takes `factory.*.temp`. We'll monitor data in both sides.

Subscribe to the live edge stream as data is received (add `--last-per-subject` to start just with the last msg per subject):

```sh
# T2-A
# edge
nats sub --stream=EDGE_TELEMETRY
```

The cloud stream also holds the other attendees' readings:

```sh
# T2-B
# cloud
nats --server demo.nats.io stream view WORKSHOP_TELEMETRY
```

You can filter by your subjects:

```sh
# T2-B
# cloud
[ -n "${WS:-}" ] || { echo "ERROR: WS must be defined and unique for you" >&2; false; } && \
nats --server demo.nats.io stream view WORKSHOP_TELEMETRY --subject "$WS.factory.>"
```

And to subscribe to the live stream as data is received, starting with the last message per subject for *your* edge:

```sh
# T2-B
# cloud
[ -n "${WS:-}" ] || { echo "ERROR: WS must be defined and unique for you" >&2; false; } && \
nats --server demo.nats.io sub --stream=WORKSHOP_TELEMETRY "$WS.factory.>" --last-per-subject
```

### WAN outage

Simulate a WAN outage. Turning off Wi-Fi would do it, but it would also drop you from the workshop call :-) and break the stream subscriber in terminal T2-B. Instead, disable the leaf remote and tell the edge to reload (no restart; local clients such as T2-A stay connected):

```sh
# T3
perl -pi -e 's/disabled: false/disabled: true/' conf/edge.conf
kill -HUP $(cat data/edge.pid)      # or: nats-server --signal reload
```

The edge server log shows `Reloaded: LeafNode Remote ... Disabled value is: true` and `Leafnode connection closed`; `curl -s localhost:8222/leafz` reports `"leafnodes": 0`.

Local traffic is unaffected. Publish some readings and watch the edge stream capture them (T2-A):

```sh
# T3
mosquitto_pub -t "factory/line1/temp" -m "offline data bit 1" -q 1
mosquitto_pub -t "factory/line2/temp" -m "offline data bit 2" -q 1
```

The edge stream grows (check terminal T2-A), but the cloud stream stays at the baseline (nothing new from you in T2-B) and your source's `Last Seen` gets older. Other attendees' readings may still arrive through their own leaf links:

```sh
# T3
nats --server demo.nats.io stream info WORKSHOP_TELEMETRY
```

Outcome: a plain leaf link **does not queue messages while disconnected**: a plain cloud subscriber would never see `offline data bit 1` and `offline data bit 2` (at-most-once). The sourced stream closes that gap: store and forward is a natural fit for edges that come and go.

### Recovery

Re-enable the leaf connection:

```sh
# T3
perl -pi -e 's/disabled: true/disabled: false/' conf/edge.conf
kill -HUP $(cat data/edge.pid)
```

The edge log shows `Leafnode connection created` again. The source retries periodically (with back-off), so you may have to wait up to a minute before the missing readings are in the cloud. Eventually the missing events will show up in the cloud subscriber in T2-B. Verify it, and also check the stream sourcing status, `Last Seen` for your source got to near cero again.

```sh
# T3
nats --server demo.nats.io stream info WORKSHOP_TELEMETRY
```

Expected: the cloud stream holds now your `online data...` and `offline data...` events, each with a `Nats-Stream-Source` header pointing to the edge stream, and your `WS` id injected as the first subject token by the subject transform. The cloud tracks how far it has read from each edge separately, and resumes from there when the link comes back.

Some things to keep in mind for real deployments:

- Size the edge stream limits (`--max-bytes`, `--max-age`...) for the longest outage you want to survive.
- The flow can be reversed (edge sourcing commands/config from the cloud).
- In production, run your own cloud cluster, add TLS and authentication to the leaf link, and usually bind it to a dedicated account with strict limits on which events flow up or down.
- [Synadia Protect](https://www.synadia.com/protect) hardens edge-to-cloud NATS connectivity (clients and leafs) with zero-trust security—without exposing NATS directly to the internet.

When done, remove your source from the shared stream (don't delete the stream; others are using it):

```sh
# T3
[ -n "${WS:-}" ] || { echo "ERROR: WS must be defined and unique for you" >&2; false; } && \
nats --server demo.nats.io stream info WORKSHOP_TELEMETRY --json \
  | jq --arg api "\$JS.EDGE-$WS.API" '.config | .sources |= map(select(.external.api != $api))' \
  > conf/workshop-telemetry-edit.json && \
nats --server demo.nats.io stream edit WORKSHOP_TELEMETRY --config conf/workshop-telemetry-edit.json -f
```

From one factory to a global fleet:

- [Leaf nodes](https://docs.nats.io/learn/topologies/leaf-nodes) — bridge a site to a cluster with a leaf that dials out and binds to an account.
- [Mirrors and sources](https://docs.nats.io/learn/jetstream/mirrors-and-sources) — copy one stream, or aggregate many edges into one.
- [JetStream `domain`](https://docs.nats.io/reference/config/jetstream/domain) — why each edge gets its own domain, and how the cloud addresses it.
- [Putting it together](https://docs.nats.io/learn/topologies/putting-it-together) — clusters, super-clusters and leaf nodes composed into one topology.
- [Config management](https://docs.nats.io/learn/deployment/config-management) — includes and live reloads with no downtime.

---

There's much, much more! Contact us -- <info@synadia.com> // <https://www.synadia.com/contact> -- we are the creators & maintainers of NATS.