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
