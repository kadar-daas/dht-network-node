// IN2011 Computer Networks
// Coursework 2024/2025
//
// Submission by
//  Kadar Daas
//  240052662
//  kadar.daas@city.ac.uk


import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.net.NetworkInterface;
import java.util.Collections;


// DO NOT EDIT starts
// This gives the interface that your code must implement.
// These descriptions are intended to help you understand how the interface
// will be used. See the RFC for how the protocol works.



interface NodeInterface {

    /* These methods configure your node.
     * They must both be called once after the node has been created but
     * before it is used. */

    // Set the name of the node.
    public void setNodeName(String nodeName) throws Exception;

    // Open a UDP port for sending and receiving messages.
    public void openPort(int portNumber) throws Exception;


    /*
     * These methods query and change how the network is used.
     */

    // Handle all incoming messages.
    // If you wait for more than delay miliseconds and
    // there are no new incoming messages return.
    // If delay is zero then wait for an unlimited amount of time.
    public void handleIncomingMessages(int delay) throws Exception;

    // Determines if a node can be contacted and is responding correctly.
    // Handles any messages that have arrived.
    public boolean isActive(String nodeName) throws Exception;

    // You need to keep a stack of nodes that are used to relay messages.
    // The base of the stack is the first node to be used as a relay.
    // The first node must relay to the second node and so on.

    // Adds a node name to a stack of nodes used to relay all future messages.
    public void pushRelay(String nodeName) throws Exception;

    // Pops the top entry from the stack of nodes used for relaying.
    // No effect if the stack is empty
    public void popRelay() throws Exception;


    /*
     * These methods provide access to the basic functionality of
     * CRN-25 network.
     */

    // Checks if there is an entry in the network with the given key.
    // Handles any messages that have arrived.
    public boolean exists(String key) throws Exception;

    // Reads the entry stored in the network for key.
    // If there is a value, return it.
    // If there isn't a value, return null.
    // Handles any messages that have arrived.
    public String read(String key) throws Exception;

    // Sets key to be value.
    // Returns true if it worked, false if it didn't.
    // Handles any messages that have arrived.
    public boolean write(String key, String value) throws Exception;

    // If key is set to currentValue change it to newValue.
    // Returns true if it worked, false if it didn't.
    // Handles any messages that have arrived.
    public boolean CAS(String key, String currentValue, String newValue) throws Exception;

}
// DO NOT EDIT ends

// Complete this!


public class Node implements NodeInterface {

    // -------------------------------------------------------------------------
    // Node identity
    // -------------------------------------------------------------------------
    private String nodeName;
    private byte[] nodeHashID;
    private DatagramSocket socket;
    private int portNumber;

    // -------------------------------------------------------------------------
    // Storage: address pairs and data pairs
    // key -> value  (both N: and D: keys live here)
    // -------------------------------------------------------------------------
    private final ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();

    // -------------------------------------------------------------------------
    // Known node addresses: nodeName -> "ip:port"
    // -------------------------------------------------------------------------
    private final ConcurrentHashMap<String, String> addressBook = new ConcurrentHashMap<>();

    // -------------------------------------------------------------------------
    // Pending requests: txid (2-char string) -> response payload
    // PENDING_SENTINEL means "waiting", any other value means "arrived"
    // -------------------------------------------------------------------------
    private static final String PENDING_SENTINEL = "\u0000";
    private final ConcurrentHashMap<String, String> pending = new ConcurrentHashMap<>();

    // -------------------------------------------------------------------------
    // Relay stack
    // -------------------------------------------------------------------------
    private final Stack<String> relayStack = new Stack<>();

    // -------------------------------------------------------------------------
    // Transaction ID counter - both bytes must never be 0x20 (space)
    // Valid range per byte: 0x21..0xFF (skipping 0x20)
    // -------------------------------------------------------------------------
    private int txCounter = 0;

    // -------------------------------------------------------------------------
    // Constants
    // -------------------------------------------------------------------------
    private static final int MAX_PACKET   = 65507;
    private static final int TIMEOUT_MS   = 5000;
    private static final int MAX_RETRIES  = 3;
    private static final int MAX_PER_DISTANCE = 3;
    private static final int MAX_RELAY_HOPS   = 10;

    // =========================================================================
    // NodeInterface - configuration
    // =========================================================================

    public void setNodeName(String nodeName) throws Exception {
        this.nodeName = nodeName;
        this.nodeHashID = HashID.computeHashID(nodeName);
    }

    public void openPort(int portNumber) throws Exception {
        this.portNumber = portNumber;
        this.socket = new DatagramSocket(portNumber);
        this.socket.setSoTimeout(100);

        String addr = getLocalAddress() + ":" + portNumber;
        store.put(nodeName, addr);
        addressBook.put(nodeName, addr);
    }

    // =========================================================================
    // NodeInterface - message handling
    // =========================================================================

    public void handleIncomingMessages(int delay) throws Exception {
        long deadline = (delay == 0) ? Long.MAX_VALUE : System.currentTimeMillis() + delay;
        while (System.currentTimeMillis() < deadline) {
            try {
                byte[] buf = new byte[MAX_PACKET];
                DatagramPacket pkt = new DatagramPacket(buf, buf.length);
                socket.receive(pkt);
                String msg = new String(pkt.getData(), 0, pkt.getLength(), StandardCharsets.UTF_8);
                processIncoming(msg, pkt.getAddress(), pkt.getPort());
            } catch (java.net.SocketTimeoutException e) {
                // No packet this iteration - check deadline
            }
        }
    }

    // =========================================================================
    // NodeInterface - relay stack
    // =========================================================================

    public void pushRelay(String nodeName) throws Exception {
        relayStack.push(nodeName);
    }

    public void popRelay() throws Exception {
        if (!relayStack.isEmpty()) {
            relayStack.pop();
        }
    }

    // =========================================================================
    // NodeInterface - network queries
    // =========================================================================

    public boolean isActive(String targetName) throws Exception {
        String addr = resolveAddress(targetName);
        if (addr == null) return false;
        String[] parts = addr.split(":");
        InetAddress ip = InetAddress.getByName(parts[0]);
        int port = Integer.parseInt(parts[1]);

        String txid = nextTxID();
        String msg = txid + " G ";
        String response = sendWithRetry(txid, msg, ip, port);
        if (response == null) return false;
        String payload = stripTxAndType(response, txid, 'H');
        if (payload == null) return false;
        String name = decodeString(payload.trim());
        return targetName.equals(name);
    }

    public boolean exists(String key) throws Exception {
        handleIncomingMessages(1);
        if (store.containsKey(key)) return true;
        List<String[]> closest = getClosestKnownNodes(HashID.computeHashID(key), 3);
        for (String[] entry : closest) {
            String addr = entry[1];
            String[] parts = addr.split(":");
            InetAddress ip = InetAddress.getByName(parts[0]);
            int port = Integer.parseInt(parts[1]);

            String txid = nextTxID();
            String msg = txid + " E " + encodeString(key) + " ";
            String response = sendWithRetry(txid, msg, ip, port);
            if (response == null) continue;
            String payload = stripTxAndType(response, txid, 'F');
            if (payload == null) continue;
            String code = payload.trim();
            if (code.equals("Y")) return true;
            if (code.equals("N")) return false;
            // '?' means this node is not close enough - try another
        }
        return false;
    }

    public String read(String key) throws Exception {
        handleIncomingMessages(1);
        if (store.containsKey(key)) return store.get(key);
        List<String[]> closest = getClosestKnownNodes(HashID.computeHashID(key), 3);
        for (String[] entry : closest) {
            String addr = entry[1];
            String[] parts = addr.split(":");
            InetAddress ip = InetAddress.getByName(parts[0]);
            int port = Integer.parseInt(parts[1]);

            String txid = nextTxID();
            String msg = txid + " R " + encodeString(key) + " ";
            String response = sendWithRetry(txid, msg, ip, port);
            if (response == null) continue;
            String payload = stripTxAndType(response, txid, 'S');
            if (payload == null) continue;
            payload = payload.trim();
            if (payload.startsWith("Y ")) {
                return decodeString(payload.substring(2));
            }
            if (payload.equals("N")) return null;
            // '?' - try another node
        }
        return null;
    }

    public boolean write(String key, String value) throws Exception {
        handleIncomingMessages(1);
        byte[] keyHash = HashID.computeHashID(key);
        List<String[]> closest = getClosestKnownNodes(keyHash, 3);
        boolean anySuccess = false;
        for (String[] entry : closest) {
            String addr = entry[1];
            String[] parts = addr.split(":");
            InetAddress ip = InetAddress.getByName(parts[0]);
            int port = Integer.parseInt(parts[1]);

            String txid = nextTxID();
            String msg = txid + " W " + encodeString(key) + " " + encodeString(value) + " ";
            String response = sendWithRetry(txid, msg, ip, port);
            if (response == null) continue;
            String payload = stripTxAndType(response, txid, 'X');
            if (payload == null) continue;
            String code = payload.trim();
            if (code.equals("A") || code.equals("R")) {
                anySuccess = true;
            }
        }
        // Also store locally if this node is one of the closest
        if (shouldStore(keyHash)) {
            store.put(key, value);
            anySuccess = true;
        }
        return anySuccess;
    }

    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        handleIncomingMessages(1);
        byte[] keyHash = HashID.computeHashID(key);
        List<String[]> closest = getClosestKnownNodes(keyHash, 3);
        for (String[] entry : closest) {
            String addr = entry[1];
            String[] parts = addr.split(":");
            InetAddress ip = InetAddress.getByName(parts[0]);
            int port = Integer.parseInt(parts[1]);

            String txid = nextTxID();
            String msg = txid + " C " + encodeString(key) + " "
                    + encodeString(currentValue) + " " + encodeString(newValue) + " ";
            String response = sendWithRetry(txid, msg, ip, port);
            if (response == null) continue;
            String payload = stripTxAndType(response, txid, 'D');
            if (payload == null) continue;
            String code = payload.trim();
            if (code.equals("R") || code.equals("A")) return true;
            if (code.equals("N")) return false;
            // '?' - try another node
        }
        // Try locally if we store this key
        if (store.containsKey(key)) {
            synchronized (store) {
                String current = store.get(key);
                if (current != null && current.equals(currentValue)) {
                    store.put(key, newValue);
                    return true;
                } else {
                    return false;
                }
            }
        }
        return false;
    }

    // =========================================================================
    // Incoming message dispatcher
    // =========================================================================

    private void processIncoming(String msg, InetAddress senderIP, int senderPort) {
        try {
            if (msg.length() < 4) return;
            if (msg.charAt(2) != ' ') return;
            String txid = msg.substring(0, 2);

            if (txid.charAt(0) == ' ' || txid.charAt(1) == ' ') return;
            char type = msg.charAt(3);
            String rest = msg.length() > 5 ? msg.substring(5) : "";

            switch (type) {
                case 'G': handleNameRequest(txid, senderIP, senderPort); break;
                case 'H': handleResponse(txid, msg); break;
                case 'N': handleNearestRequest(txid, rest, senderIP, senderPort); break;
                case 'O': handleResponse(txid, msg); break;
                case 'E': handleExistenceRequest(txid, rest, senderIP, senderPort); break;
                case 'F': handleResponse(txid, msg); break;
                case 'R': handleReadRequest(txid, rest, senderIP, senderPort); break;
                case 'S': handleResponse(txid, msg); break;
                case 'W': handleWriteRequest(txid, rest, senderIP, senderPort); break;
                case 'X': handleResponse(txid, msg); break;
                case 'C': handleCASRequest(txid, rest, senderIP, senderPort); break;
                case 'D': handleResponse(txid, msg); break;
                case 'V': handleRelayRequest(txid, rest, senderIP, senderPort, 0); break;
                case 'I': break;
                default:  break;
            }
        } catch (Exception e) {
            // never crash on a malformed message
        }
    }

    // =========================================================================
    // Request handlers (incoming from other nodes)
    // =========================================================================

    private void handleNameRequest(String txid, InetAddress ip, int port) throws Exception {
        // G -> H + nodeName
        String response = txid + " H " + encodeString(nodeName) + " ";
        sendRaw(response, ip, port);
        // Passive mapping: learn the sender's name and address in a background thread
        InetAddress capturedIP = ip;
        int capturedPort = port;
        Thread t = new Thread(() -> {
            try {
                String askTxid = nextTxID();
                String askMsg = askTxid + " G ";
                String askResponse = sendWithRetryDirect(askTxid, askMsg, capturedIP, capturedPort);
                if (askResponse == null) return;
                String payload = stripTxAndType(askResponse, askTxid, 'H');
                if (payload == null) return;
                String senderName = decodeString(payload.trim());
                if (senderName != null && senderName.startsWith("N:")) {
                    String senderAddr = capturedIP.getHostAddress() + ":" + capturedPort;
                    learnAddress(senderName, senderAddr);
                }
            } catch (Exception e) { /* ignore */ }
        });
        t.setDaemon(true);
        t.start();
    }

    private void handleNearestRequest(String txid, String rest, InetAddress ip, int port) throws Exception {

        String hashHex = rest.trim();
        if (hashHex.length() != 64) return;
        byte[] targetHash = hexToBytes(hashHex);

        List<String[]> closest = getClosestKnownNodes(targetHash, 3);
        StringBuilder sb = new StringBuilder();
        sb.append(txid).append(" O ");
        for (String[] entry : closest) {
            sb.append(encodeString(entry[0])).append(" ").append(encodeString(entry[1])).append(" ");
        }
        sendRaw(sb.toString(), ip, port);
    }

    private void handleExistenceRequest(String txid, String rest, InetAddress ip, int port) throws Exception {

        int[] pos = new int[]{0};
        String key = decodeStringAt(rest, pos);
        if (key == null) return;
        boolean hasKey = store.containsKey(key);
        boolean isClose = isOneOfThreeClosest(HashID.computeHashID(key));
        String code;
        if (hasKey) code = "Y";
        else if (isClose) code = "N";
        else code = "?";
        String response = txid + " F " + code + " ";
        sendRaw(response, ip, port);
    }

    private void handleReadRequest(String txid, String rest, InetAddress ip, int port) throws Exception {

        int[] pos = new int[]{0};
        String key = decodeStringAt(rest, pos);
        if (key == null) return;
        boolean hasKey = store.containsKey(key);
        boolean isClose = isOneOfThreeClosest(HashID.computeHashID(key));
        String response;
        if (hasKey) {
            response = txid + " S Y " + encodeString(store.get(key)) + " ";
        } else if (isClose) {
            response = txid + " S N ";
        } else {
            response = txid + " S ? ";
        }
        sendRaw(response, ip, port);
    }

    private void handleWriteRequest(String txid, String rest, InetAddress ip, int port) throws Exception {

        int[] pos = new int[]{0};
        String key = decodeStringAt(rest, pos);
        if (key == null) return;
        String value = decodeStringAt(rest, pos);
        if (value == null) return;

        boolean hasKey = store.containsKey(key);
        boolean isClose = isOneOfThreeClosest(HashID.computeHashID(key));
        String code;
        if (hasKey) {
            store.put(key, value);
            code = "R";
        } else if (isClose) {
            store.put(key, value);
            if (key.startsWith("N:")) {
                addressBook.put(key, value);
            }
            code = "A";
        } else {
            code = "X";
        }
        String response = txid + " X " + code + " ";
        sendRaw(response, ip, port);
    }

    private void handleCASRequest(String txid, String rest, InetAddress ip, int port) throws Exception {

        int[] pos = new int[]{0};
        String key = decodeStringAt(rest, pos);
        if (key == null) return;
        String requestedValue = decodeStringAt(rest, pos);
        if (requestedValue == null) return;
        String newValue = decodeStringAt(rest, pos);
        if (newValue == null) return;

        boolean isClose = isOneOfThreeClosest(HashID.computeHashID(key));
        String code;
        synchronized (store) {
            boolean hasKey = store.containsKey(key);
            if (hasKey) {
                String current = store.get(key);
                if (current.equals(requestedValue)) {
                    store.put(key, newValue);
                    code = "R";
                } else {
                    code = "N";
                }
            } else if (isClose) {
                store.put(key, newValue);
                code = "A";
            } else {
                code = "X";
            }
        }
        String response = txid + " D " + code + " ";
        sendRaw(response, ip, port);
    }


    private void handleRelayRequest(String txid, String rest, InetAddress senderIP, int senderPort, int hopCount) {

        if (hopCount >= MAX_RELAY_HOPS) return;
        String capturedRest = rest;
        String capturedTxid = txid;
        InetAddress capturedIP = senderIP;
        int capturedPort = senderPort;
        int nextHop = hopCount + 1;
        Thread t = new Thread(() -> {
            try {
                int[] pos = new int[]{0};
                String targetName = decodeStringAt(capturedRest, pos);
                if (targetName == null) return;
                // The embedded message starts at pos[0]; it has its own txid and type
                String embedded = capturedRest.substring(pos[0]);
                if (embedded.length() < 4) return;

                // Guard: do not relay back to ourselves to prevent loops
                if (targetName.equals(nodeName)) return;

                String addr = resolveAddress(targetName);
                if (addr == null) return;
                String[] parts = addr.split(":");
                InetAddress targetIP = InetAddress.getByName(parts[0]);
                int targetPort = Integer.parseInt(parts[1]);

                // Rewrite the transaction ID in the embedded message with a fresh one
                String innerTxid = nextTxID();
                String rewritten = innerTxid + embedded.substring(2);

                String response = sendWithRetryDirect(innerTxid, rewritten, targetIP, targetPort);
                if (response == null) return;

                // Return response to original sender using the relay transaction ID
                String forwarded = capturedTxid + response.substring(2);
                sendRaw(forwarded, capturedIP, capturedPort);
            } catch (Exception e) {

            }
        });
        t.setDaemon(true);
        t.start();
    }

    private void handleResponse(String txid, String msg) {
        pending.put(txid, msg);
    }

    // =========================================================================
    // UDP send helpers
    // =========================================================================

    private String sendWithRetry(String txid, String msg, InetAddress ip, int port) throws Exception {
        if (!relayStack.isEmpty()) {
            return sendViaRelay(txid, msg, ip, port);
        }
        return sendWithRetryDirect(txid, msg, ip, port);
    }

    private String sendWithRetryDirect(String txid, String msg, InetAddress ip, int port) throws Exception {
        pending.put(txid, PENDING_SENTINEL);
        for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {
            sendRaw(msg, ip, port);
            long deadline = System.currentTimeMillis() + TIMEOUT_MS;
            while (System.currentTimeMillis() < deadline) {
                drainIncoming();
                String response = pending.get(txid);
                if (response != null && !response.equals(PENDING_SENTINEL)) {
                    pending.remove(txid);
                    return response;
                }
                Thread.sleep(50);
            }
        }
        pending.remove(txid);
        return null;
    }

    private String sendViaRelay(String txid, String msg, InetAddress targetIP, int targetPort) throws Exception {
        // Build relay chain: relayStack index 0 = first hop
        String targetName = getNodeNameForAddress(targetIP.getHostAddress() + ":" + targetPort);
        if (targetName == null) targetName = "N:unknown";

        List<String> chain = new ArrayList<>(relayStack);

        // Wrap from inside out: innermost is the actual message to target
        String wrappedMsg = msg;
        for (int i = chain.size() - 1; i >= 0; i--) {
            String nextTarget = (i == chain.size() - 1) ? targetName : chain.get(i + 1);
            String relayTxid = (i == 0) ? txid : nextTxID();
            wrappedMsg = relayTxid + " V " + encodeString(nextTarget) + " " + wrappedMsg;
        }

        String firstRelayAddr = resolveAddress(chain.get(0));
        if (firstRelayAddr == null) return null;
        String[] parts = firstRelayAddr.split(":");
        InetAddress relayIP = InetAddress.getByName(parts[0]);
        int relayPort = Integer.parseInt(parts[1]);

        return sendWithRetryDirect(txid, wrappedMsg, relayIP, relayPort);
    }

    private void sendRaw(String msg, InetAddress ip, int port) throws Exception {
        byte[] data = msg.getBytes(StandardCharsets.UTF_8);
        DatagramPacket pkt = new DatagramPacket(data, data.length, ip, port);
        socket.send(pkt);
    }

    private void drainIncoming() {
        try {
            byte[] buf = new byte[MAX_PACKET];
            DatagramPacket pkt = new DatagramPacket(buf, buf.length);
            socket.receive(pkt);
            String msg = new String(pkt.getData(), 0, pkt.getLength(), StandardCharsets.UTF_8);
            processIncoming(msg, pkt.getAddress(), pkt.getPort());
        } catch (java.net.SocketTimeoutException e) {
            // Nothing waiting
        } catch (Exception e) {
            // Ignore
        }
    }

    // =========================================================================
    // Address resolution and nearest-node logic
    // =========================================================================

    private String resolveAddress(String targetName) throws Exception {
        if (addressBook.containsKey(targetName)) {
            return addressBook.get(targetName);
        }
        byte[] targetHash = HashID.computeHashID(targetName);
        List<String[]> candidates = getClosestKnownNodes(targetHash, 3);
        for (String[] entry : candidates) {
            String addr = entry[1];
            String[] parts = addr.split(":");
            InetAddress ip = InetAddress.getByName(parts[0]);
            int port = Integer.parseInt(parts[1]);
            String txid = nextTxID();
            String msg = txid + " N " + bytesToHex(targetHash) + " ";
            String response = sendWithRetryDirect(txid, msg, ip, port);
            if (response == null) continue;
            String payload = stripTxAndType(response, txid, 'O');
            if (payload == null) continue;
            learnAddressPairsFromPayload(payload);
            if (addressBook.containsKey(targetName)) {
                return addressBook.get(targetName);
            }
        }
        return null;
    }

    private void learnAddressPairsFromPayload(String payload) {
        int[] pos = new int[]{0};
        while (pos[0] < payload.length()) {
            String key = decodeStringAt(payload, pos);
            if (key == null) break;
            String value = decodeStringAt(payload, pos);
            if (value == null) break;
            if (key.startsWith("N:")) {
                learnAddress(key, value);
            }
        }
    }

    private void learnAddress(String name, String addr) {
        if (name == null || addr == null) return;
        if (addressBook.containsKey(name)) {
            addressBook.put(name, addr);
            return;
        }
        try {
            byte[] targetHash = HashID.computeHashID(name);
            int dist = distance(nodeHashID, targetHash);
            int count = 0;
            for (String n : addressBook.keySet()) {
                try {
                    int d = distance(nodeHashID, HashID.computeHashID(n));
                    if (d == dist) count++;
                } catch (Exception e) { /* ignore */ }
            }
            if (count < MAX_PER_DISTANCE) {
                addressBook.put(name, addr);
            }
        } catch (Exception e) { /* ignore */ }
    }

    private List<String[]> getClosestKnownNodes(byte[] targetHash, int n) {
        List<String[]> result = new ArrayList<>();
        for (ConcurrentHashMap.Entry<String, String> entry : addressBook.entrySet()) {
            try {
                byte[] h = HashID.computeHashID(entry.getKey());
                int dist = distance(targetHash, h);
                result.add(new String[]{entry.getKey(), entry.getValue(), String.valueOf(dist)});
            } catch (Exception e) { /* skip */ }
        }
        result.sort((a, b) -> Integer.compare(Integer.parseInt(a[2]), Integer.parseInt(b[2])));
        List<String[]> top = new ArrayList<>();
        for (int i = 0; i < Math.min(n, result.size()); i++) {
            top.add(new String[]{result.get(i)[0], result.get(i)[1]});
        }
        return top;
    }

    private boolean isOneOfThreeClosest(byte[] keyHash) {
        try {
            int myDist = distance(this.nodeHashID, keyHash);
            int strictlyCloser = 0;

            for (String knownName : addressBook.keySet()) {
                // Skip comparing against yourself
                if (knownName.equals(this.nodeName)) continue;

                // Use the official HashID class
                byte[] otherHash = HashID.computeHashID(knownName);
                int otherDist = distance(otherHash, keyHash);

                if (otherDist < myDist) {
                    strictlyCloser++;
                }
            }

            // If 3 or more nodes are strictly closer, you are at best the 4th closest
            return strictlyCloser < 3;
        } catch (Exception e) {
            // prevent the node from crashing on a hashing error
            return false;
        }
    }

    private boolean shouldStore(byte[] keyHash) {
        return isOneOfThreeClosest(keyHash);
    }

    private String getNodeNameForAddress(String addr) {
        for (ConcurrentHashMap.Entry<String, String> entry : addressBook.entrySet()) {
            if (entry.getValue().equals(addr)) return entry.getKey();
        }
        return null;
    }

    // =========================================================================
    // CRN String encoding / decoding
    // =========================================================================
    // RFC format: number_of_spaces + SPACE + UTF-8_string + SPACE (delimiter)
    // "Hello World" -> "1 Hello World "   (1 internal space, trailing delimiter)
    // "Hello"       -> "0 Hello "
    // ""            -> "0  "  (count=0, then space, then empty, then delimiter)

    private String encodeString(String s) {
        if (s == null) s = "";
        int spaces = 0;
        for (char c : s.toCharArray()) {
            if (c == ' ') spaces++;
        }
        return spaces + " " + s;
    }

    private String decodeString(String encoded) {

        int firstSpace = encoded.indexOf(' ');
        if (firstSpace == -1) return "";
        return encoded.substring(firstSpace + 1);
    }



    private String decodeStringAt(String s, int[] pos) {
        if (pos[0] >= s.length()) return null;
        int start = pos[0];
        // Find the space that separates the count number from the value
        int spaceIdx = s.indexOf(' ', start);
        if (spaceIdx == -1) return null;
        int count;
        try {
            count = Integer.parseInt(s.substring(start, spaceIdx));
        } catch (NumberFormatException e) {
            return null;
        }
        // Value starts immediately after the separating space
        int valueStart = spaceIdx + 1;
        int idx = valueStart;
        int spacesFound = 0;
        // Walk forward until we have consumed exactly `count` internal spaces
        while (idx < s.length()) {
            if (s.charAt(idx) == ' ') {
                if (spacesFound == count) {

                    break;
                }
                spacesFound++;
            }
            idx++;
        }
        String value = s.substring(valueStart, idx);
        // Skip past the trailing delimiter space
        if (idx < s.length() && s.charAt(idx) == ' ') idx++;
        pos[0] = idx;
        return value;
    }

    // =========================================================================
    // Hash and distance helpers
    // =========================================================================



    private int distance(byte[] a, byte[] b) {
        int matchingBits = 0;
        // Iterate through each byte of the 32-byte (256-bit) hashes
        for (int i = 0; i < 32; i++) {
            // XOR shows which bits are different (0 if same, 1 if different)
            int xor = (a[i] & 0xFF) ^ (b[i] & 0xFF);

            if (xor == 0) {
                // All 8 bits in this byte match
                matchingBits += 8;
            } else {

                int leadingZerosInByte = Integer.numberOfLeadingZeros(xor) - 24;
                matchingBits += leadingZerosInByte;
                break; // Stop at the first difference
            }
        }
        // The protocol defines distance as 256 minus the prefix match
        return 256 - matchingBits;
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xFF));
        }
        return sb.toString();
    }

    private byte[] hexToBytes(String hex) {
        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                    + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
    }

    // =========================================================================
    // Transaction ID helpers
    // =========================================================================

    private synchronized String nextTxID() {
        int a = txCounter;
        txCounter = (txCounter + 1) % (0xDE * 0xDE);
        // Map counter to two bytes each in range 0x21..0xFE (skipping 0x20)
        int b1 = (a / 0xDE) + 0x21;
        int b2 = (a % 0xDE) + 0x21;
        // Safety clamp: ensure neither byte is accidentally 0x20
        if (b1 == 0x20) b1 = 0x21;
        if (b2 == 0x20) b2 = 0x21;
        return "" + (char) b1 + (char) b2;
    }

    // Strip txid and type from a response; return payload or null if mismatch
    private String stripTxAndType(String response, String txid, char expectedType) {
        if (response == null || response.length() < 4) return null;
        if (!response.startsWith(txid)) return null;
        if (response.charAt(2) != ' ') return null;
        if (response.charAt(3) != expectedType) return null;
        return response.length() > 5 ? response.substring(5) : "";
    }

    // =========================================================================
    // Network interface helper
    // =========================================================================

    private String getLocalAddress() {
        try {
            List<NetworkInterface> interfaces = Collections.list(NetworkInterface.getNetworkInterfaces());
            for (NetworkInterface iface : interfaces) {
                if (iface.isLoopback() || !iface.isUp()) continue;
                List<InetAddress> addresses = Collections.list(iface.getInetAddresses());
                for (InetAddress addr : addresses) {
                    if (addr instanceof java.net.Inet4Address && !addr.isLoopbackAddress()) {
                        return addr.getHostAddress();
                    }
                }
            }
        } catch (Exception e) { /* fall through */ }
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            return "127.0.0.1";
        }
    }

}
