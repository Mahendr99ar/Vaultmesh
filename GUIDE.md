# VaultMesh — Step-by-Step Build and Hosting Guide

This guide walks through the project in the order you should understand it, explains **what you
are doing and why** at every step, shows how to test each piece, and ends with hosting on AWS,
resume bullets and interview questions.

> **Important:** Amazon interviewers ask deep questions about resume projects. Do not just copy
> this code. Go phase by phase, read every file, run every test, break things on purpose, and be
> able to explain each design choice in your own words. The "Explain it in the interview" notes
> are there for that.

---

## Part 0 — What you are building, in one paragraph

A key-value database (like a tiny DynamoDB) that keeps working when machines crash. Keys are
spread across **shards** using **consistent hashing**. Each shard is stored on **3 nodes**, and
the nodes agree on every write using the **Raft consensus algorithm**: one node is the
**leader**, it writes the change to its **write-ahead log (WAL)** on disk, copies it to the other
two, and only says "OK" to the client once a **majority (2 of 3)** has it on disk. If the leader
dies, the other two **elect a new leader** within about a second, and nothing that was
acknowledged is lost. A **gateway** in front hides all of this from the client.

**How this matches the Amazon job description**

| Job description says | Where it is in this project |
|---|---|
| distributed storage, index and query systems | sharded, replicated KV store with a hash-ring index |
| scalable | add shards; consistent hashing moves only ~1/N of keys |
| fault-tolerant | Raft replication, leader failover, WAL crash recovery, chaos test |
| operating systems | `fsync`, file offsets, `ftruncate`, atomic `rename`, event loop |
| algorithms, data structures, complexity | sorted ring with binary search O(log V), log indexing, majority counting |
| object-oriented design | `RaftNode`, `WriteAheadLog`, `Transport` protocol with two implementations |
| distributed, multi-tiered systems | client → gateway → Raft groups → disk |
| AWS | Docker deployment on EC2 |

---

## Part 1 — Set up your machine

1. Install **Python 3.10 or newer** (python.org; on Windows tick "Add python.exe to PATH").
2. Install **Git** and **VS Code**.
3. Open a terminal in the project folder and create a virtual environment:

   ```bash
   python -m venv .venv
   # Windows:      .venv\Scripts\activate
   # macOS/Linux:  source .venv/bin/activate
   pip install -r requirements-dev.txt
   ```

4. Check everything works:

   ```bash
   python -m pytest -v
   ```

   You should see **22 passed**.

---

## Part 2 — The build, phase by phase

Read the phases in order. Each one says which file to read, what it does, and how to test it.

### Phase 1 — Write-ahead log (`vaultmesh/wal.py`)

**What you are doing:** building an append-only file where every Raft log entry is stored before
the node tells anyone "I have it".

**How it works**

* Each record on disk is `[length 4 bytes][crc32 4 bytes][payload]`. The payload is the entry's
  term and command bytes.
* `append()` writes a whole batch with **one** `os.write` and **one** `os.fsync`. `fsync` forces
  the operating system to push its page cache to the physical disk; without it, a power cut
  could lose data the node already promised to keep.
* On startup `_recover()` reads the file record by record. If the last record is incomplete
  (the process died mid-write) or its CRC does not match, it is cut off with `ftruncate`.
  This is called **torn-write recovery**.
* `truncate_from()` deletes a suffix of the log. Followers need this when an old leader sent
  them entries that never got committed.
* Entries are also kept in a Python list, so reading never touches the disk. Index 0 is a
  virtual entry with term 0, which simplifies the Raft code.

**Test it:** `python -m pytest tests/test_wal.py -v`. One test writes half a record on purpose
to simulate a crash; another flips bytes to prove the CRC catches corruption.

**Explain it in the interview:** "Why do you need fsync if you already called write?" →
`write` only copies to the OS page cache; `fsync` makes it durable. "Why a CRC?" → to detect a
partially written or corrupted record after a crash instead of reading garbage.

### Phase 2 — Durable term and vote (`vaultmesh/state.py`)

**What you are doing:** saving `current_term` and `voted_for` so they survive restarts.

**Why:** if a node voted for A in term 5, crashed and came back, it must not vote for B in term 5.
Two votes in one term could elect two leaders.

**How:** write a temp file → `fsync` → `os.replace` (atomic rename). A crash at any moment
leaves either the old file or the new one, never a half-written mix.

### Phase 3 — Messages and transport (`messages.py`, `transport.py`)

**What you are doing:** defining the two Raft RPCs and how nodes send them.

* `RequestVote(term, candidate_id, last_log_index, last_log_term)`
* `AppendEntries(term, leader_id, prev_log_index, prev_log_term, entries, leader_commit)`
  — with no entries it is a **heartbeat**.

`Transport` is a `Protocol` (an interface). `HttpTransport` sends JSON over HTTP with a pooled
`aiohttp` client. The tests use a different implementation, an in-memory network that can drop
messages. This is the **dependency inversion** principle: `RaftNode` does not care how bytes
travel, which is what makes it testable.

### Phase 4 — Leader election (`vaultmesh/raft.py`)

Read these methods: `_reset_election_timer`, `_start_election`, `_request_vote`,
`handle_request_vote`, `_become_leader`, `_become_follower`.

**How it works**

1. Every node starts as a **follower** with a random timeout (300–600 ms).
2. If it hears nothing from a leader before the timeout, it becomes a **candidate**: increments
   the term, votes for itself (persisted), and asks everyone else for a vote.
3. A node grants its vote only if (a) it has not voted for someone else this term **and**
   (b) the candidate's log is at least as up to date as its own (higher last term, or same last
   term and at least as long). Rule (b) guarantees a new leader already holds every committed
   entry.
4. With a majority of votes the candidate becomes **leader** and immediately starts heartbeats
   (every 75 ms) so nobody else times out.
5. Anyone who sees a higher term in any message steps down to follower.

**Why random timeouts?** If all nodes timed out together they would all vote for themselves
forever (split votes). Randomness makes one node usually go first.

**Test it:** `python -m pytest tests/test_raft.py -k "leader" -v`

### Phase 5 — Log replication and commit (`vaultmesh/raft.py`)

Read: `_replicate_to`, `_append_loop`, `_on_append_response`, `_advance_commit_index`,
`handle_append_entries`.

**Leader side**

* For each follower the leader tracks `next_index` (next entry to send) and `match_index`
  (highest entry known to be stored there).
* It sends entries starting at `next_index`, together with the index and term of the entry just
  before them (`prev_log_index`, `prev_log_term`).
* If the follower says "mismatch", the leader moves `next_index` back. The follower returns a
  hint (`last_log_index`) so the leader can jump back many entries in one step.
* Only **one** request per follower is in flight; if new data arrives meanwhile, a `dirty` flag
  makes the loop send again right after. This avoids flooding a slow follower.
* An entry is **committed** once a majority stores it **and** it is from the leader's current
  term. That is why a new leader first appends a **no-op** entry: committing it also commits
  everything before it safely. (This is the subtle "Figure 8" case in the Raft paper — a great
  interview talking point.)

**Follower side** (`handle_append_entries`)

1. Reject if the leader's term is old.
2. Check that it has the entry at `prev_log_index` with `prev_log_term`. If not, reject.
3. Delete any conflicting entries (`truncate_from`) and append the new ones.
4. Move its commit index forward to the leader's and apply newly committed entries.

**Test it:** `python -m pytest tests/test_raft.py -v`. Look at
`test_isolated_minority_leader_cannot_commit`: the old leader is cut off, its write never
commits, and when it reconnects its uncommitted entry is thrown away.

### Phase 6 — The key-value state machine (`vaultmesh/kv.py`)

**What you are doing:** the actual database. A `Command` (`put`/`delete`, key, value) is encoded
to bytes and stored in the Raft log. Once committed, every node calls `KVStateMachine.apply` in
the same order, so all three copies end up identical. On restart the dict is rebuilt by
replaying the log.

### Phase 7 — Client API: group commit and ReadIndex (`vaultmesh/raft.py`)

**`submit()` — writes with group commit:** writes go into a queue. The first write in an
event-loop tick schedules `_flush_write_queue`, which appends **all** queued writes with a single
`fsync`. Under load, hundreds of writes share one disk flush. The caller's future completes only
after the entry is committed and applied.

**`read_barrier()` — linearizable reads (ReadIndex):** a leader cannot simply read its own memory,
because it might have been replaced by a new leader without knowing it (a network partition).
So a read:

1. records the current commit index,
2. sends a heartbeat round and waits until a **majority** answers, which proves it is still
   leader,
3. waits until the state machine has applied up to that index, then reads.

This is cheaper than writing every read to the log. `?consistency=stale` skips all of this for
a fast but possibly slightly old answer.

**Test it:** `test_isolated_leader_cannot_serve_strong_reads` shows a cut-off leader refusing to
answer instead of returning stale data.

### Phase 8 — The node HTTP server (`vaultmesh/node_server.py`)

FastAPI app with Raft endpoints (`/raft/vote`, `/raft/append`) and client endpoints
(`/kv/{key}`, `/status`). If a client reaches a follower, it answers `503` with the header
`X-Raft-Leader: host:port` so the caller can go to the right node.

### Phase 9 — Sharding and the gateway (`ring.py`, `gateway.py`)

**Consistent hashing (`ring.py`):** each shard is placed on a ring 128 times (**virtual nodes**)
at positions from an MD5 hash. A key goes to the first shard position clockwise from its own
hash, found with binary search (`bisect`) in O(log V).

Why not `hash(key) % N`? Going from 3 to 4 shards would move about 75% of all keys.
With the ring only about 25% (1/N) move, and only onto the new shard. `test_ring.py` proves this.
Virtual nodes keep the load even (the test checks each shard is within 15% of its fair share).

**Gateway (`gateway.py`):** finds the shard for a key, sends the request to the cached leader,
follows `X-Raft-Leader` redirects, and if a node is dead tries the next replica with exponential
backoff (50 ms, 100 ms, 200 ms…). The client never sees a failover; it just sees one slower
request. The `X-Attempts` response header shows how many tries it took.

### Phase 10 — Live view (`vaultmesh/static/dashboard.html`, `/admin/crash`)

A single HTML page served at `/` (no build step, no framework), styled like an ambient
"clock" screen.

* **The sky is the cluster.** A canvas draws each shard as a three-star constellation: the
  leader glows amber, heartbeat pulses travel from it to the followers, and a crashed node
  flickers red. It is real data from `/cluster`, polled twice a second.
* **Big type in the centre** shows the focused shard's leader and term, for example
  `s1n2 · term 3`. After you crash a leader it switches to `s1n1 · 740 ms`: the new leader and
  the failover time measured from your click.
* **Control bar:** previous shard, start/stop traffic, next shard, and the red lightning button
  that crashes the focused shard's leader for 4 seconds. Keyboard: arrows, space, C.
  The crash calls `POST /admin/crash`, which makes the leader go silent (`RaftNode.pause`) exactly
  like a dead machine. The gateway refuses unless all three replicas are healthy, so a visitor can
  never take a shard below its majority. When the old leader returns it sees the higher term and
  steps down.
* **Console below the fold:** replica table, live traffic chart (requests per second, p99, and a
  red line where you crashed a leader), a plain-language timeline, and the hash ring with a
  read/write form.
* Night and morning themes, works on phones, respects "reduce motion".

This is the best live demo you can give an interviewer: open it, press play, press the lightning
button.

### Phase 11 — Testing strategy (`tests/`)

* `conftest.py` builds an **in-memory network**. `isolate(node)` cuts a node off from everyone,
  like unplugging its cable. This makes failure scenarios repeatable in milliseconds.
* `test_raft.py` covers election, replication, group commit, failover, minority partitions,
  linearizable reads, full-cluster restart from the WAL, follower catch-up, a 5-node cluster
  surviving 2 failures, and log-conflict truncation.
* Run the suite many times to catch timing bugs:

  ```bash
  # macOS/Linux
  for i in $(seq 1 20); do python -m pytest -q | tail -1; done
  ```

### Phase 12 — Run the real cluster, break it, measure it

1. Start 6 nodes + gateway: `python scripts/local_cluster.py --clean`
2. Open http://localhost:8080 and try PUT/GET.
3. In a second terminal run the chaos test:

   ```bash
   python scripts/chaos_test.py --rounds 5
   ```

   It writes continuously, **SIGKILLs** shard1's leader, measures how long until writes work
   again, restarts the dead node (it recovers from its WAL), then reads back every acknowledged
   write. Expected output ends with `RESULT: PASS - no acknowledged write was lost`.
4. Benchmark:

   ```bash
   python -m vaultmesh bench --ops 10000 --concurrency 64
   ```

5. **Write down your own numbers.** They depend on your machine. Put your measured numbers on
   your resume, not the ones in this guide.

**A real profiling story to tell:** the first version used the `httpx` HTTP client and reached
only about 160 writes/sec. Even reads that skip Raft were slow, which pointed away from the
consensus code. Benchmarking a bare `/health` endpoint with two different clients showed `httpx`
capped at about 300 req/s while `aiohttp` handled 5,800 req/s. Switching the gateway and the
node-to-node RPC to `aiohttp` gave about **9×** more throughput. Disabling per-request access
logging (heartbeats every 75 ms were filling the disk with log lines) was also part of the fix.
Interviewers love "how did you find the bottleneck" stories; this one is real.

### Phase 13 — Docker (`Dockerfile`, `docker-compose.yml`)

* `Dockerfile` builds one image with Python and the code. Dependencies are installed before the
  code is copied, so Docker caches that layer.
* `docker-compose.yml` runs 7 containers from that image: `s1n1..s1n3`, `s2n1..s2n3` and
  `gateway`. Each node has its own **named volume** at `/data`, so its WAL survives container
  restarts. Only the gateway's port 8080 is published.
* Commands:

  ```bash
  docker compose up -d --build      # start
  docker compose ps                 # see containers
  docker compose logs -f s1n1       # watch one node (elections are logged)
  docker compose kill s1n1          # crash a node
  docker compose start s1n1         # bring it back
  docker compose down               # stop (add -v to also delete the data volumes)
  ```

### Phase 14 — Put it on GitHub with CI

```bash
git init
git add .
git commit -m "VaultMesh: sharded Raft key-value store"
git branch -M main
git remote add origin https://github.com/Mahendr99ar/vaultmesh.git   # create the empty repo on GitHub first
git push -u origin main
```

`.github/workflows/ci.yml` runs on every push: the 22 tests, a 3-round chaos test on a real
cluster, and a Docker build. A green CI badge on the README shows the project actually works.
To add the badge, put this at the top of `README.md`:

```markdown
![CI](https://github.com/Mahendr99ar/vaultmesh/actions/workflows/ci.yml/badge.svg)
```

Good commit habit: commit after each phase above with a clear message. A real history of 15–20
commits looks far better to a reviewer than one giant commit.

---

## Part 3A — Free hosting (no credit card): Render

Render's free web service gives a public HTTPS link and builds straight from your GitHub repo.
(Hugging Face Spaces now needs a paid plan for Docker, so it is no longer the free option.)

The free plan has 512 MB RAM and a small share of a CPU, which is too little for 7 separate
processes (about 440 MB). So the Docker image runs **demo mode** by default:
`python -m vaultmesh demo`. It runs all 6 Raft nodes and the gateway in **one** process. The Raft
code and write-ahead log are the same; nodes pass Raft messages through an in-process network
(`vaultmesh/demo.py`, `LocalTransport`) instead of HTTP, and the gateway still talks to the nodes
over real HTTP. Measured: **64 MB RAM and about 1% CPU when idle.**

**Limits to know (and to say honestly in an interview):**
* The service **sleeps after 15 minutes without visitors**; the next visit takes about a minute to
  wake it. Open the link yourself a minute before an interview.
* The disk is not persistent. A redeploy or restart starts with an empty store; it is a demo.
* All nodes share one machine, so this shows the algorithm, not real hardware failures. The
  multi-process mode (`scripts/local_cluster.py`) and Docker Compose run real separate processes.

**Steps**

1. Go to **render.com** → **Get Started** → **Sign up with GitHub** (no card needed).
2. Allow Render to see your repositories (all, or only `vaultmesh`).
3. Dashboard → **New +** → **Web Service** → pick **Mahendr99ar/vaultmesh** → **Connect**.
4. Fill in:
   * **Name:** `vaultmesh` (your link becomes `https://vaultmesh.onrender.com`, or with a suffix
     if the name is taken)
   * **Language / Runtime:** **Docker** (it finds the `Dockerfile` in the repo root)
   * **Branch:** `main`
   * **Region:** Singapore (closest to India)
   * **Instance type:** **Free**
5. Click **Deploy Web Service**. The log shows the Docker build; after 3–5 minutes it says
   **Your service is live**. Open the link at the top of the page.
6. Every `git push` to `main` redeploys automatically.

To test the same thing on your laptop: `python -m vaultmesh demo` and open http://localhost:8080.

### Other free options

| Option | Cost | Good for | Catch |
|---|---|---|---|
| **Your own laptop** | Free | Interview demos: `local_cluster.py` plus the crash button or `chaos_test.py` | Not online 24/7 |
| **GitHub Actions** (already set up) | Free | Proof it works: every push runs the tests, a chaos test and the demo; green badge is public | Not a live website |
| **Render free web service** | Free, no card | Public demo link, auto-deploy from GitHub | Sleeps after 15 min idle, 512 MB, no persistent disk |
| **Koyeb free instance** | Free | Same idea as Render (Docker, 512 MB) | Sleeps after 1 h idle; may ask for a card to verify |
| **AWS Free plan** (Part 3 below) | $0 on the Free plan, using credits | Real cloud experience; "deployed on AWS" on the resume | Needs a card; account suspends after 6 months unless upgraded |
| **Oracle Cloud Always Free** | Free forever | A real VM running Docker Compose exactly like Part 3 | Needs a card; free ARM capacity is often "out of capacity" |

**Recommended free combination:** GitHub (code + CI badge) + Render (live link) + a 60-second
screen recording of the crash button. That costs nothing and shows everything.

---

## Part 3 — Host it on AWS (EC2 + Docker Compose)

This puts the whole cluster on one AWS server with a public dashboard link you can put on your
resume. On the AWS **Free plan** you are not charged: usage comes out of the sign-up credits,
and the account is suspended (not billed) after 6 months or when credits run out, unless you
choose to upgrade. Do not upgrade to the Paid plan if you want to stay at $0.

### Step 1 — AWS account and a budget alarm

1. Create an AWS account at aws.amazon.com (needs a card; new accounts currently get starter
   credits — check the Free Tier page for the exact current offer).
2. **Do this first:** Billing and Cost Management → Budgets → Create budget → "Zero spend" or a
   monthly budget of $5 with email alerts. This protects you from surprise bills.
3. Pick the **Mumbai (ap-south-1)** region in the top-right corner (lowest latency from India).

### Step 2 — Launch the server

EC2 → Instances → **Launch instance**:

| Setting | Value |
|---|---|
| Name | `vaultmesh` |
| AMI | **Ubuntu Server 24.04 LTS** |
| Instance type | **t3.small** (2 GB RAM) recommended; **t3.micro** (1 GB) also works because each container is capped at 128 MB |
| Key pair | Create new → `vaultmesh` → download the `.pem` file and keep it safe |
| Network settings | Allow SSH from **My IP**. Then "Edit" → **Add security group rule**: Type *Custom TCP*, Port **8080**, Source **My IP** (change to *Anywhere* only when you want to share the link) |
| Storage | 20 GB gp3 |

Click **Launch instance**, open it, and copy its **Public IPv4 address**.

### Step 3 — Connect to the server

Easiest: select the instance → **Connect** → **EC2 Instance Connect** → Connect. A terminal opens
in your browser.

Or from your own terminal (works in Windows PowerShell too):

```bash
ssh -i vaultmesh.pem ubuntu@<PUBLIC-IP>
```

(If Windows complains about the key's permissions, right-click the .pem → Properties → Security
→ Advanced → disable inheritance and leave only your user.)

### Step 4 — Install Docker

```bash
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker ubuntu
exit
```

Reconnect (so the group change applies), then check:

```bash
docker --version
docker compose version
```

### Step 5 — Deploy

```bash
git clone https://github.com/Mahendr99ar/vaultmesh.git
cd vaultmesh
docker compose up -d --build
docker compose ps                         # all 7 should be "running"
curl -s localhost:8080/cluster | head -c 400
```

Open **http://\<PUBLIC-IP\>:8080** in your browser. You should see both shards, each with one
leader and two followers.

### Step 6 — Prove fault tolerance on the server

```bash
sudo apt-get install -y python3-aiohttp
python3 scripts/chaos_test.py --docker --rounds 3
```

Or do it by hand while the dashboard is open: run `docker compose kill s1n1` (or whichever node
is leader) and watch a follower become leader within a second; then `docker compose start s1n1`
and watch it rejoin as a follower and catch up.

### Step 7 — Updating after code changes

```bash
cd vaultmesh
git pull
docker compose up -d --build
```

### Step 8 — Keep the link stable and costs low

* The public IP changes every time you stop and start the instance. For a fixed address,
  EC2 → **Elastic IPs** → Allocate → Associate with the instance. Note that AWS charges for
  public IPv4 addresses (roughly $3.60/month), whether Elastic or not.
* Optional free domain: create a subdomain at duckdns.org pointing to your IP, e.g.
  `http://mahendra-vaultmesh.duckdns.org:8080`.
* **Stop** the instance when you are not using it (you then pay only for the small disk).
  **Terminate** it when placements are over.
* Security: the demo has no login, so anyone with the link can write keys. Keep port 8080
  limited to **My IP** normally and open it to everyone only when you share the link. Never
  store real personal data in it.

### Step 9 (optional, advanced) — Real multi-machine deployment

For true fault tolerance across data centres, launch **3 instances in 3 Availability Zones**
(ap-south-1a, 1b, 1c) in the same VPC, open port 9000 between them (security group rule with
the security group itself as source), and run one node of each shard per machine using
**private IPs**. On each machine, install Docker (Step 4), clone the repo and build the image with
`docker build -t vaultmesh .`. Then on machine A (private IP 10.0.1.10, with B = 10.0.2.10,
C = 10.0.3.10):

```bash
docker run -d --name s1n1 --restart unless-stopped -p 9101:9101 -v s1n1:/data vaultmesh \
  node --id s1n1 --port 9101 --data /data --peers s1n2=10.0.2.10:9101,s1n3=10.0.3.10:9101
docker run -d --name s2n1 --restart unless-stopped -p 9201:9201 -v s2n1:/data vaultmesh \
  node --id s2n1 --port 9201 --data /data --peers s2n2=10.0.2.10:9201,s2n3=10.0.3.10:9201
```

Repeat on B and C with their own ids, run the gateway on any machine with
`--shards "shard1=10.0.1.10:9101,10.0.2.10:9101,10.0.3.10:9101;shard2=10.0.1.10:9201,10.0.2.10:9201,10.0.3.10:9201"`,
then stop a whole instance and watch the cluster survive an Availability Zone failure. This costs
3× more, so do it once, take a screenshot or record a short video, and shut it down.

---

## Part 4 — Show it off

1. Record a 60–90 second screen video: dashboard open → PUT a key → `docker compose kill` the
   leader → a new leader appears → GET the key still works → restart the node → it catches up.
   Upload to YouTube (unlisted) or LinkedIn and link it in the README.
2. Put a screenshot of the dashboard at the top of the README.
3. Pin the repository on your GitHub profile.

---

## Part 5 — Resume bullets

Replace the numbers with **your own measurements** from Phase 12.

> **VaultMesh — Distributed Fault-Tolerant Key-Value Store** · Python, asyncio, FastAPI, Docker, AWS EC2 · [GitHub] [Live demo]
> * Implemented the **Raft consensus algorithm** from scratch (leader election, log replication, majority commit, ReadIndex linearizable reads) and sharded data across Raft groups via a **consistent-hashing** gateway with 128 virtual nodes, moving only ~1/N of keys on rebalance.
> * Built a crash-safe **write-ahead log** (CRC32 checksums, fsync, torn-write recovery) with group commit; profiled and removed an HTTP-client bottleneck to raise throughput **9×** to **1,400 writes/sec** (p99 93 ms).
> * Verified with **22 pytest** tests (network partitions, crash recovery) and a chaos test that SIGKILLs leaders under load: **0 of 10,000+** acknowledged writes lost, **~650 ms** average failover; deployed with **Docker Compose on AWS EC2** with GitHub Actions CI.

---

## Part 6 — Interview questions you should be able to answer

1. **What happens step by step when a client writes `PUT user:42`?** Gateway hashes the key →
   shard1 → sends to cached leader → leader queues the command → group commit appends to WAL
   with fsync → sends AppendEntries → followers check `prev_log_index/term`, append, fsync, reply
   → leader sees a majority → commit index advances → state machine applies → client gets 200.
2. **What if the leader crashes after writing to its own log but before replicating?** The entry
   was never acknowledged; a new leader without it is elected and the entry is overwritten when
   the old leader rejoins. No acknowledged write is lost.
3. **Why does a 3-node cluster tolerate only 1 failure, and a 5-node cluster 2?** A majority is
   needed; any two majorities overlap in at least one node, which carries the committed data.
4. **What is split brain and how does Raft prevent it?** At most one leader per term (one vote
   per node per term, persisted), and a leader cut off in a minority cannot commit because it
   cannot reach a majority. Shown by `test_isolated_minority_leader_cannot_commit`.
5. **Why can't a leader serve reads from memory directly?** It may already be deposed without
   knowing. ReadIndex confirms leadership with a majority first.
6. **Why consistent hashing instead of modulo?** Adding a shard moves ~1/N keys instead of most.
   Virtual nodes even out the distribution.
7. **What does fsync cost, and how did you reduce it?** A disk flush per write is slow; group
   commit shares one fsync among all writes arriving in the same event-loop tick.
8. **How is concurrency handled without locks?** Each node runs on a single asyncio event loop
   and changes state only in synchronous methods; network replies are validated against the
   current term.
9. **Time and space complexity:** ring lookup O(log V); commit check O(N·k) over peers; WAL
   recovery O(file size); the log is O(entries) in memory.
10. **What would you build next?** Snapshots and log compaction, dynamic membership changes,
    shard rebalancing with data migration, client request IDs for exactly-once writes, metrics
    with Prometheus.

---

## Suggested two-week schedule

| Days | Work |
|---|---|
| 1–2 | Read the Raft paper sections 1–5 and watch an animation (thesecretlivesofdata.com/raft). Phases 1–3 |
| 3–5 | Phases 4–5: election and replication. Break the code on purpose and watch tests fail |
| 6–7 | Phases 6–8: state machine, group commit, ReadIndex, HTTP server |
| 8–9 | Phase 9–10: hash ring, gateway, dashboard |
| 10 | Phases 11–12: tests, chaos test, benchmark, record your numbers |
| 11–12 | Phases 13–14: Docker, GitHub, CI |
| 13 | Part 3: AWS hosting |
| 14 | Part 4–6: video, resume bullets, practise answering the interview questions out loud |
