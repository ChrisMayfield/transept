# Deployment

How to set up the third-party hosted mode on a rented server, which supports multiple rooms.
`HOSTED.md` is the design record behind it, for why any of this is shaped the way it is.

Four files go on the server: three from this directory, and the fourth the project's own settings example.

| File                     | Goes to                            | What it is                               |
| ------------------------ | ---------------------------------- | ---------------------------------------- |
| `Caddyfile`              | `/etc/caddy/Caddyfile`             | TLS, and which room answers which prefix |
| `transept@.service`      | `/etc/systemd/system/`             | a single room as a systemd template      |
| `room.example.toml`      | `/srv/transept/<room>/config.toml` | that room's ports, tokens, and name      |
| `../config.example.toml` | `/srv/transept/config.toml`        | the keys and tuning every room shares    |

Each room lives under its own name: `https://transept.example.org/<room>/reader`.
Caddy strips the `<room>` prefix before forwarding, so the server behind it serves `/reader` and `/control` exactly as it does on a laptop.
The room name is the same value as the working directory, the systemd instance, and `[server] room`.
One hostname, one certificate, one DNS record, however many rooms.

## Domain name

Caddy requires a domain name to obtain an HTTPS certificate.
Point an A record at the VM's address and let it take effect before you start.
Caddy asks Let's Encrypt for a certificate the moment it loads a config.
A hostname that does not yet resolve costs a failed validation and a wait.

## Server setup

Debian or Ubuntu, with a `transept` user account, the checkout owned by root, and only each room's directory writable.
(See [The room](#the-room) below, which is where those permissions are set).

```sh
sudo git clone https://github.com/ChrisMayfield/transept.git /srv/transept
sudo adduser --system --group --no-create-home --home /srv/transept --shell /usr/sbin/nologin transept
sudo python3 -m venv /srv/transept/.venv
sudo /srv/transept/.venv/bin/pip install -r /srv/transept/requirements.txt
```

No `ffmpeg` is needed on this machine.
Audio that arrives is passed through untouched whatever `[audio] encoding` says, and the laptop is where the Opus encoder belongs.
A laptop with no `ffmpeg` sends raw audio, and this server, having none either, forwards that untouched too and says so in the journal.

## Firewall

Open two ports and nothing else, since every room's listeners bind loopback, and Caddy is the only public surface.
Note: This step might not be necessary if a firewall is already provided by the web host.

```sh
sudo ufw allow OpenSSH
sudo ufw allow 80,443/tcp
sudo ufw enable
```

## Shared config

The ordinary settings file, the same one a laptop runs on:

```sh
sudo cp /srv/transept/config.example.toml /srv/transept/config.toml
sudo nano /srv/transept/config.toml

sudo chown root:transept /srv/transept/config.toml
sudo chmod 0640 /srv/transept/config.toml
```

Four lines to change (with `nano`), and the file's own comments explain each of them:

- `deepgram_api_key` and `llm_api_key`, which live on this server rather than on the operators' laptops.
- `backend`, from `auto` to `remote`, which is the whole of what makes this the hosted half.
  There is no sound card here: the audio arrives over the control socket from `sender.py` on a laptop in the room.
  Each room raises the control listener in place of the operator one because of this line.
  If left at `auto`, every room comes up with an operator page on loopback and nothing for a sender to connect to.
- `public_url`, the bare hostname Caddy answers on, with no room in it: each room adds its own name as the path segment.

A different translation provider is two more, `llm_base_url` and `model`, which the file already fills in for Google.
Neither may be left empty: there is no built-in default model, and a server exits without one.

The language list stays here too, because the same congregation meeting in two rooms likely uses the same languages.
Anything about a tunnel or the operator page in those comments is about a laptop and does not apply to this machine.

Every room's unit reads this file and then the room's own, merged key by key, so anything a room differs on it says for itself and inherits the rest.
That is what keeps changing the translation provider one edit per server rather than one edit per room.

## Room config

In the following example, the room is named `chapel`.

```sh
sudo mkdir -p /srv/transept/chapel
sudo cp /srv/transept/deploy/room.example.toml /srv/transept/chapel/config.toml
sudo nano /srv/transept/chapel/config.toml

sudo chown root:transept /srv/transept/chapel /srv/transept/chapel/config.toml
sudo chmod 3770 /srv/transept/chapel
sudo chmod 0640 /srv/transept/chapel/config.toml
```

The directory belongs to root and the `transept` group, not to the `transept` user, which is worth getting right.
The server has to create `sessions.db` and `transept.pid` in that directory, so the group needs to write it.
But it has no business rewriting the keys it was given, so `config.toml` stays root's and is only readable.

The two extra bits on `3770` are what make those permissions hold.
_Setgid_, so `sessions.db` is created in the `transept` group rather than inheriting whatever the server's primary group happens to be.
_Sticky_, because write permission on a directory is otherwise permission to delete anything in it whatever the file's own mode says.
(A server that cannot edit `config.toml` but can replace it is a server that can still change its own `llm_base_url`.)
Anything else the room reads rather than writes, a `glossary.txt` or a `keyterms.txt`, belongs to root the same way.

Four values have to be filled in: `reader_token`, `control_token`, `room`, and the two ports, which have to differ from every other room's.
You can generate the tokens rather than inventing them:

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

Change `transept.example.org` to the real hostname in two files: here and in `public_url`.
Delete the `classroom` block until there is a second room (see [A second room](#a-second-room) below).
The prefix and the two ports in a room's block have to match that room's `config.toml`.

## Systemd

Systemd runs Transept as a service, starts it automatically, and keeps it running if it stops.

```sh
sudo cp /srv/transept/deploy/transept@.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now transept@chapel
journalctl -u transept@chapel -n 30
```

The journal holds the reader address with its token and its prefix, printed once at startup, which is the address that room's card carries.
It is there rather than in `transept.log` because systemd is the supervisor here, in place of the `transept` script.

## Testing

The server should be up and running now.
Test it by running [`check_backend.sh`](../check_backend.sh).
This script issues four web requests that between them cover what each listener serves and what it turns away.

## Second room

In the following example, the room is named `classroom`.

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

The ports must differ from the first room's; the tokens should differ because per-room tokens are the whole access boundary; the name must differ because it is what tells the rooms apart.
Everything else is inherited, so a room that meets in a second language adds a `[languages] available` of its own and nothing more.

Restarting one room does not touch the others, which is the reason each room is a separate process rather than a registry entry.
The operating system gives the isolation for free, and a handler that blocks stalls one room's subtitles instead of every room's.

## Starting a session

The room's laptop needs the following settings in `config.toml` (the rest can be left unchanged):

```toml
[keys]
control_token = "..."     # the same value as this room's server side

[audio]
backend = "auto"
encoding = "opus"         # the leg worth compressing is this one

[server]
public_url = "https://transept.example.org/"
room = "chapel"
```

The address the laptop dials is built from the last two: `wss://transept.example.org/chapel/control`.
The room's name is in the url because a hosted room lives under a path prefix.
A laptop dialing the wrong prefix reaches the wrong room, which refuses the connection.

Run `python sender.py` on the laptop, which prints the operator address on loopback and connects to the backend server.
The address to hand the room appears on the operator page once the sender has reached the server, because the server is what holds the reader token.

## Troubleshooting

If `systemctl status transept@chapel` says **failed** rather than activating, because the unit gives up after five tries in a minute.
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
