# deploy

Steps 4 and 5 of the order of work in `HOSTED.md`: one room on a rented server, and then the rooms after it.

Four files go on the server, three from this directory and the fourth the project's own settings example, and none of them names a room except the copy you make of the last.

| File                     | Goes to                            | What it is                               |
| ------------------------ | ---------------------------------- | ---------------------------------------- |
| `Caddyfile`              | `/etc/caddy/Caddyfile`             | TLS, and which room answers which prefix |
| `transept@.service`      | `/etc/systemd/system/`             | a room as a systemd template             |
| `../config.example.toml` | `/srv/transept/config.toml`        | the keys and tuning every room shares    |
| `room.example.toml`      | `/srv/transept/<room>/config.toml` | that room's ports, tokens, and name      |

Each room lives under its own name: `https://transept.example.org/chapel/reader`.
Caddy strips that prefix before forwarding, so the server behind it serves `/reader` and `/control` exactly as it does on a laptop, and the name is the same value as the working directory, the systemd instance, and `[server] room`.
One hostname, one certificate, one DNS record, however many rooms.

## Before the server

Point an A record at the VM's address and let it take effect before you start Caddy, because Caddy asks Let's Encrypt for a certificate the moment it loads a config with a real hostname.
A hostname that does not yet resolve costs a failed validation and a wait.

## The server

Debian or Ubuntu, as the `transept` user, with the checkout owned by root and only each room's directory writable (see [The room](#the-room) below, which is where those permissions are set).

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
Only if you install `ffmpeg` here is `[audio] encoding = "pcm"` worth adding to the shared file, to stop a second codec pass that would cost latency to save nothing.

Open two ports and nothing else, since every room's listeners bind loopback and Caddy is the only public surface.

```sh
sudo ufw allow OpenSSH
sudo ufw allow 80,443/tcp
sudo ufw enable
```

## What every room shares

The ordinary settings file, the same one a laptop runs on, which is why there is no hosted copy of it to keep in step:

```sh
sudo cp /srv/transept/config.example.toml /srv/transept/config.toml
sudo nano /srv/transept/config.toml

sudo chown root:transept /srv/transept/config.toml
sudo chmod 0640 /srv/transept/config.toml
```

Four lines to change, and the file's own comments explain each of them:

- `deepgram_api_key` and `llm_api_key`, which live on this server rather than on the operators' laptops, and are most of the point of hosting.
- `backend`, from `auto` to `remote`, which is the whole of what makes this the hosted half. There is no sound card here: the audio arrives over the control socket from `sender.py` on a laptop in the room, and each room raises the control listener in place of the operator one because of this line. Left at `auto`, every room comes up with an operator page on loopback and nothing for a sender to connect to.
- `public_url`, the bare hostname Caddy answers on, with no room in it: each room adds its own name as the path segment after it.

A different translation provider is two more, `llm_base_url` and `model`, which the file already fills in for Google.
Neither may be left empty: there is no built-in default model and a server exits without one.

The language list stays here too, because the same congregation meeting in two rooms wants the same languages, and everything else in the file is tuning every room inherits.
Anything about a tunnel or the operator page in those comments is about a laptop and does not apply to this machine.

Every room's unit reads this file and then the room's own, merged key by key, so anything a room differs on it says for itself and inherits the rest.
That is what keeps changing the translation provider one edit rather than one edit per room.

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

Four values have to be filled in: `reader_token`, `control_token`, `room`, and the two ports, which have to differ from every other room's because the rooms share a machine.
Mint the tokens rather than inventing them:

```sh
python3 -c "import secrets; print(secrets.token_urlsafe(32))"
```

The directory name, the `[server] room` setting, the systemd instance, and the path prefix in the `Caddyfile` are all `chapel` on purpose.
One value naming all four is one value that cannot disagree with itself.

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

Change `transept.example.org` to the real hostname in two files, here and in `public_url`, and delete the `classroom` block until there is a second room.
The hostname has to match, because one is what a phone connects to and the other is what the QR code tells it to connect to.
The prefix and the two ports in a room's block have to match that room's `config.toml` for the same reason.

## The unit

```sh
sudo cp /srv/transept/deploy/transept@.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now transept@chapel
journalctl -u transept@chapel -n 30
```

The journal holds the reader address with its token and its prefix, printed once at startup, which is the address that room's card carries.
It is there rather than in `transept.log` because systemd is the supervisor here, in place of the `transept` script.

## Checking it

Four requests, all from your own machine, that between them cover what each listener serves and what it turns away.

```sh
ROOM=https://transept.example.org/chapel
READER=...      # reader_token from the room's config
CONTROL=...     # control_token from the room's config

curl -sI https://transept.example.org/            # 404, the bot that finds it
curl -s  $ROOM/api/channels                       # 403, no token
curl -s  "$ROOM/api/channels?token=$READER"       # the language list
curl -s  -H "Origin: https://example.com" \
         "$ROOM/control?token=$CONTROL"           # Not a route for a browser.
```

The last one is the check worth doing, because the control socket is the one thing here that a token alone guards.
A browser stamps every websocket handshake with an `Origin` and cannot be made not to, so a refusal of the header is what stands between a leaked control token and a page that could use it.

Then open `$ROOM/reader?token=$READER` in a browser.
A room that answers `/api/channels` but shows a blank language picker is a page asking at the wrong address, which is what the prefix is about: the page derives what to ask for from its own address rather than from the root.

## A second room

No code, and nothing the first room has to be told about.

```sh
sudo mkdir -p /srv/transept/classroom
sudo cp /srv/transept/deploy/room.example.toml /srv/transept/classroom/config.toml
sudo nano /srv/transept/classroom/config.toml    # room, both tokens, both ports
sudo chown root:transept /srv/transept/classroom /srv/transept/classroom/config.toml
sudo chmod 3770 /srv/transept/classroom
sudo chmod 0640 /srv/transept/classroom/config.toml

sudo nano /etc/caddy/Caddyfile                   # its prefix and its two ports
sudo caddy validate --config /etc/caddy/Caddyfile --adapter caddyfile
sudo systemctl reload caddy

sudo systemctl enable --now transept@classroom
```

The ports must differ from the first room's, the tokens must differ because per-room tokens are the whole access boundary, and the name must differ because it is what tells the rooms apart.
Everything else is inherited, so a room that meets in a second language adds a `[languages] available` of its own and nothing more.

Restarting one room does not touch the others, which is the reason a room is a process rather than a registry entry: the operating system gives the isolation for free, and a handler that blocks stalls one room's subtitles instead of every room's.

## The laptop in the room

The room's half needs three settings and one key, and no Deepgram or translation key at all.

```toml
[keys]
control_token = "..."     # the same value as this room's server side

[audio]
backend = "auto"
encoding = "opus"         # the leg worth compressing is this one

[server]
control_url = "wss://transept.example.org/chapel/control"
room = "chapel"
```

The room's name is in `control_url` because a hosted room lives under a path prefix, and a laptop dialing the wrong prefix reaches the wrong room, which refuses it: a sender says which room it is in its hello, and a room takes one sender.

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

**Two rooms answer the same address.**
A room's ports in the `Caddyfile` and in its `config.toml` have drifted apart, so one prefix reaches the other room's server, with that room's readers and that room's token.
The ports in a room's block are the ones in that room's file, and nowhere else.

**Caddy will not start.**
Run `caddy validate` before `systemctl reload caddy`, and check the A record resolves.

**A reader gets 503.**
`max_readers` is 100 by default, and a reader who closed the page holds its slot until the next keepalive write fails, up to 15 seconds later.

## Coming from the first version of this file

A room deployed when this file described one room at the root of a hostname needs three edits to move under its own prefix, in this order:

- Split its `config.toml`, leaving the keys, the model, the backend, the languages, and `public_url` in `/srv/transept/config.toml` and keeping the tokens, the ports, and `room` in the room's own, then copy `transept@.service` over the old unit for its two `--config` arguments and `systemctl daemon-reload`.
- Wrap the `Caddyfile`'s two `handle` blocks in `handle_path /chapel/*`, and reload Caddy.
- Put `/chapel` into the laptop's `control_url`.

The address on the card changes with them, so reprint it, and `port` in any config file is now `reader_port`.
