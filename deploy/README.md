# deploy

Step 4 of the order of work in `HOSTED.md`: one room on a rented server.

Three files go on the server, and none of them names a room except the copy you make of the third.

| File                | Goes to                            | What it is                             |
| ------------------- | ---------------------------------- | -------------------------------------- |
| `Caddyfile`         | `/etc/caddy/Caddyfile`             | TLS, and which port answers which path |
| `transept@.service` | `/etc/systemd/system/`             | a room as a systemd template           |
| `room.example.toml` | `/srv/transept/<room>/config.toml` | that room's ports, keys, and tokens    |

One room lives at the root of its own hostname, which is why none of this strips a path prefix.
Rooms sharing one hostname is step 5, and the four things it needs are listed at the end of this file.

## Before the server

Point an A record at the VM's address and let it take effect before you start Caddy, because Caddy asks Let's Encrypt for a certificate the moment it loads a config with a real hostname.
A hostname that does not yet resolve costs a failed validation and a wait.

## The server

Debian or Ubuntu, as the `transept` user, with the checkout owned by root and only the room's directory writable (see [The room](#the-room) below, which is where those permissions are set).

```sh
sudo git clone https://github.com/ChrisMayfield/transept.git /srv/transept
sudo adduser --system --group --no-create-home \
     --home /srv/transept --shell /usr/sbin/nologin transept
sudo python3 -m venv /srv/transept/.venv
sudo /srv/transept/.venv/bin/pip install -r /srv/transept/requirements.txt
```

No `ffmpeg` is needed on this machine.
Audio that arrives already encoded is passed through untouched whatever `[audio] encoding` says, and the laptop is where the Opus encoder belongs, because the laptop's uplink is the leg that fails.
A laptop with no `ffmpeg` sends raw audio, and this server, having none either, forwards that untouched too and says so in the journal.
Only if you install `ffmpeg` here is `[audio] encoding = "pcm"` worth adding to the room's config, to stop a second codec pass that would cost latency to save nothing.

Open two ports and nothing else, since both of the server's listeners bind loopback and Caddy is the only public surface.

```sh
sudo ufw allow OpenSSH
sudo ufw allow 80,443/tcp
sudo ufw enable
```

## The room

```sh
sudo mkdir -p /srv/transept/chapel
sudo cp /srv/transept/deploy/room.example.toml /srv/transept/chapel/config.toml
sudo nano /srv/transept/chapel/config.toml

sudo chown root:transept /srv/transept/chapel /srv/transept/chapel/config.toml
sudo chmod 3770 /srv/transept/chapel
sudo chmod 0640 /srv/transept/chapel/config.toml
```

The directory belongs to root and the `transept` group, not to the `transept` user, which is the split worth getting right.
The server has to create `sessions.db` and `transept.pid` in that directory, so the group needs to write it.
It has no business rewriting the keys it was given, so `config.toml` stays root's and is only readable.

The two extra bits on `3770` are what make that hold.
Setgid, so `sessions.db` is created in the `transept` group rather than inheriting whatever the server's primary group happens to be.
Sticky, because write permission on a directory is otherwise permission to delete anything in it whatever the file's own mode says, and a server that cannot edit `config.toml` but can replace it is a server that can still change its own `llm_base_url`.
Anything else the room reads rather than writes, a `glossary.txt` or a `keyterms.txt`, belongs to root the same way.

Five values have to be filled in: `deepgram_api_key`, `llm_api_key`, `reader_token`, `control_token`, and `public_url`.
Everything else inherits its default, so anything this room differs on, its language list above all, is copied in from `../config.example.toml`.
Mint the two tokens rather than inventing them:

```sh
python3 -c "import secrets; print(secrets.token_urlsafe(32))"
```

The directory name, the `[server] room` setting, and the systemd instance are all `chapel` on purpose.
One value naming all three is one value that cannot disagree with itself.

## Caddy

Install Caddy from its own apt repository, because the one in Debian's is usually old enough to matter:

```sh
sudo apt install -y debian-keyring debian-archive-keyring apt-transport-https curl
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' \
  | sudo gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' \
  | sudo tee /etc/apt/sources.list.d/caddy-stable.list
sudo apt update && sudo apt install -y caddy
```

Then:

```sh
sudo cp /srv/transept/deploy/Caddyfile /etc/caddy/Caddyfile
sudo nano /etc/caddy/Caddyfile
sudo caddy validate --config /etc/caddy/Caddyfile --adapter caddyfile
sudo systemctl reload caddy
```

Change `transept.example.org` to the real hostname in two files, here and in `public_url`.
They have to match, because one is what a phone connects to and the other is what the QR code tells it to connect to.

## The unit

```sh
sudo cp /srv/transept/deploy/transept@.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now transept@chapel
journalctl -u transept@chapel -n 30
```

The journal holds the reader address with its token, printed once at startup, which is the address the room's card carries.
It is there rather than in `transept.log` because systemd is the supervisor here, in place of the `transept` script.

## Checking it

Four requests, all from your own machine, that between them cover what each listener serves and what it turns away.

```sh
HOST=https://transept.example.org
READER=...      # reader_token from the room's config
CONTROL=...     # control_token from the room's config

curl -sI $HOST/                                   # 404, the bot that finds it
curl -s  $HOST/api/channels                       # 403, no token
curl -s  "$HOST/api/channels?token=$READER"       # the language list
curl -s  -H "Origin: https://example.com" \
         "$HOST/control?token=$CONTROL"           # Not a route for a browser.
```

The last one is the check worth doing, because the control socket is the one thing here that a token alone guards.
A browser stamps every websocket handshake with an `Origin` and cannot be made not to, so a refusal of the header is what stands between a leaked control token and a page that could use it.

## The laptop in the room

The room's half needs three settings and one key, and no Deepgram or translation key at all.

```toml
[keys]
control_token = "..."     # the same value as the server's

[audio]
backend = "auto"
encoding = "opus"         # the leg worth compressing is this one

[server]
control_url = "wss://transept.example.org/control"
room = "chapel"
```

Then `python3 sender.py`, which prints the operator address on loopback and connects.
The address to hand the room appears on that page once the sender has reached the server, because the server is what holds the reader token and `public_url`.

## When it goes wrong

`systemctl status transept@chapel` says **failed** rather than activating, because the unit gives up after five tries in a minute.
That is nearly always a config that will not load, and `journalctl -u transept@chapel -n 30` says which line.

**The operator page shows a meeting running with no audio.**
The snapshot is stale and the socket is down.
`sender.py` lays the socket's own state over the server's snapshot for exactly this, so the page should say so rather than look healthy.

**A phone cannot open the link.**
`journalctl -u caddy -f` shows each request with its path and status, and the reader token hashed rather than written out.
That is enough to tell a missing token from a wrong one, since a missing one leaves the parameter out altogether, and enough to tell one card's token from another after a rotation.

**Caddy will not start.**
Run `caddy validate` before `systemctl reload caddy`, and check the A record resolves.

**A reader gets 503.**
`max_readers` is 100 by default, and a reader who closed the page holds its slot until the next keepalive write fails, up to 15 seconds later.

## What step 5 still needs

Rooms sharing one hostname under path prefixes is not configuration alone.
Four things in the code are not built:

- `--config` is a single argument, so there is no left to right merge of a shared file and a room file.
- `port` has not been renamed to `reader_port`.
- `reader_address(base, token)` takes no room, so it cannot build the prefix.
- `reader.html` builds `/stream/` and `/api/channels` as root absolute paths, which a stripped prefix breaks.

A second room on a second hostname needs none of them, and is a second copy of `room.example.toml`, a second `systemctl enable`, and a second block in the `Caddyfile`.
