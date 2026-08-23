# Binance Exchange Logs → Coralogix

Polls Binance account activity and ships it to Coralogix.

**Production cadence:** every **5 minutes**, collect the **last 5 minutes** of logs, then POST them to Coralogix.

```
:00  cron starts  →  Binance last 5 min  →  Coralogix
:05  cron starts  →  Binance last 5 min  →  Coralogix
:10  cron starts  →  Binance last 5 min  →  Coralogix
 ...
```

Each cron run is a single shot (`--once --lookback-minutes 5`). The process exits when the window is shipped. Overlaps are safe because `state.json` remembers event IDs already sent.

```
Binance USER_DATA APIs                    Coralogix Logs API
GET /sapi/v1/capital/deposit/hisrec  ┐
GET /sapi/v1/capital/withdraw/history┤ →  poller  →  POST /logs/v1/singles
GET /sapi/v1/asset/transfer          ┘                   application: binance
GET /api/v3/myTrades  (optional)                         subsystem: exchange-logs
GET /api/v3/allOrders (optional)
```

Binance does not expose a single account-audit endpoint. This integration polls the official history APIs for deposits, withdrawals, wallet transfers, and optionally spot trades/orders.

## How the 5-minute poll works

| Setting | Value | Meaning |
| --- | --- | --- |
| Cron | `*/5 * * * *` | Start a run at `:00`, `:05`, `:10`, … |
| `--once` | required | One cycle, then exit (do not stay resident) |
| `--lookback-minutes 5` | required | Ask Binance for **now − 5 minutes → now** |
| `LOOKBACK_MINUTES=5` | in `.env` | Same window if you omit the CLI flag |
| `POLL_INTERVAL_SECONDS=300` | in `.env` | Used only by the optional in-process loop |

`run.sh` already passes `--once --lookback-minutes 5`, so crontab only needs to invoke the script every 5 minutes.

Each run:

1. Syncs the host clock with `GET /api/v3/time`
2. Reads deposit, withdraw, and transfer history for the last 5 minutes
3. Drops events already recorded in `state.json`
4. POSTs new events to Coralogix Logs (`/logs/v1/singles`)
5. Updates `state.json` with shipped IDs and the latest timestamp

If a cron tick is missed (host asleep, job delayed), the next run catches up from the last checkpoint instead of dropping events. A 2-second overlap on the window covers clock skew between ticks.

## APIs used

| Direction | API | Auth |
| --- | --- | --- |
| Pull | Binance `GET /sapi/v1/capital/deposit/hisrec` | HMAC-SHA256 (`X-MBX-APIKEY` + `signature`) |
| Pull | Binance `GET /sapi/v1/capital/withdraw/history` | HMAC-SHA256 |
| Pull | Binance `GET /sapi/v1/asset/transfer` | HMAC-SHA256 |
| Pull | Binance `GET /api/v3/myTrades` (optional) | HMAC-SHA256, requires `BINANCE_SYMBOLS` |
| Pull | Binance `GET /api/v3/allOrders` (optional) | HMAC-SHA256, requires `BINANCE_SYMBOLS` |
| Push | Coralogix `POST https://ingress.<domain>/logs/v1/singles` | Send-Your-Data API key (Bearer) |

## Prerequisites

- Python 3.10+
- Binance API key with **Enable Reading** only  
  Binance → **API Management** → Create API → enable **Enable Reading**  
  Do **not** enable Spot/Futures trading or Withdrawals
- Restrict the key to the poller host public IP
- Coralogix Send-Your-Data API key  
  Coralogix → **Data Flow → API Keys**
- Coralogix domain (`eu1.coralogix.com`, `coralogix.in`, …)

## Quick start

```bash
cd binance-coralogix

python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

cp .env.example .env
chmod +x run.sh
```

Edit `.env` and put real keys there (never commit `.env`):

```bash
BINANCE_API_KEY=...
BINANCE_API_SECRET=...
BINANCE_BASE_URL=https://api.binance.com
BINANCE_SOURCES=deposit,withdraw,transfer

CORALOGIX_SEND_YOUR_DATA_KEY=...
CORALOGIX_DOMAIN=eu1.coralogix.com
CORALOGIX_APPLICATION_NAME=binance
CORALOGIX_SUBSYSTEM_NAME=exchange-logs

# last 5 minutes, every 5 minutes
POLL_INTERVAL_SECONDS=300
LOOKBACK_MINUTES=5
```

Binance.US: set `BINANCE_BASE_URL=https://api.binance.us`.

### Test one 5-minute window (no ingest)

```bash
python ship_binance_to_coralogix.py --once --dry-run --lookback-minutes 5
```

### Test one 5-minute window (send to Coralogix)

```bash
python ship_binance_to_coralogix.py --once --lookback-minutes 5
```

or:

```bash
./run.sh
```

You should see `Done. Total events shipped: N`. `N` can be `0` if the account had no deposit, withdraw, or transfer activity in the last 5 minutes.

### Optional in-process loop

```bash
python ship_binance_to_coralogix.py
```

This sleeps `POLL_INTERVAL_SECONDS` (default `300`) between cycles. Prefer **cron** in production so the job restarts after reboot and does not depend on a long-lived process.

## Optional trade / order collection

Spot trade and order history require a symbol on every Binance request:

```bash
BINANCE_SOURCES=deposit,withdraw,transfer,trade,order
BINANCE_SYMBOLS=BTCUSDT,ETHUSDT
```

## Cron setup (every 5 minutes)

Production is a **cron job every 5 minutes**. Each tick collects the last 5 minutes of Binance logs and ships them to Coralogix.

### 1. Install on the host

```bash
sudo mkdir -p /opt/binance-coralogix
sudo cp -a binance-coralogix/. /opt/binance-coralogix
cd /opt/binance-coralogix

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
chmod +x run.sh
# put real API keys in .env
```

Confirm `.env` exists in `/opt/binance-coralogix`. Cron does not load your interactive shell profile, so keys must live in that file (or in the crontab `EnvironmentFile` / systemd unit).

### 2. Confirm a manual run

```bash
/opt/binance-coralogix/run.sh
```

Expected log line: `Done. Total events shipped: N`.

`run.sh` is:

```bash
python ship_binance_to_coralogix.py --once --lookback-minutes 5
```

That is one 5-minute window, then exit.

### 3. Create the cron log file

```bash
sudo touch /var/log/binance-coralogix.log
sudo chown "$(whoami)" /var/log/binance-coralogix.log
```

### 4. Install crontab

```bash
crontab -e
```

Add this line:

```cron
# Binance exchange logs → Coralogix
# Every 5 minutes, ship the last 5 minutes of activity
*/5 * * * * /opt/binance-coralogix/run.sh >> /var/log/binance-coralogix.log 2>&1
```

What that line means:

| Part | Meaning |
| --- | --- |
| `*/5` | Minute field: `0,5,10,15,20,25,30,35,40,45,50,55` |
| `* * * *` | Every hour, every day of month, every month, every weekday |
| `/opt/binance-coralogix/run.sh` | One-shot poller (`--once --lookback-minutes 5`) |
| `>> ... 2>&1` | Append stdout and stderr to the log file |

Without the wrapper:

```cron
*/5 * * * * cd /opt/binance-coralogix && /opt/binance-coralogix/.venv/bin/python ship_binance_to_coralogix.py --once --lookback-minutes 5 >> /var/log/binance-coralogix.log 2>&1
```

Use the **absolute** venv Python path. Cron's `PATH` is minimal and will not find `.venv/bin/python` unless you `cd` and call it by full path.

### 5. Verify cron

```bash
crontab -l
tail -f /var/log/binance-coralogix.log
```

Wait until the next 5-minute boundary (`:00`, `:05`, `:10`, …). You should see a new `Polling Binance lookback=5 minute(s)` block each tick.

Useful checks:

```bash
# crontab is installed for this user
crontab -l | grep binance-coralogix

# last runs
grep "Done. Total events shipped" /var/log/binance-coralogix.log | tail

# cron daemon is running (Linux)
systemctl status cron || systemctl status crond
```

### macOS notes

- Grant **Full Disk Access** to `/usr/sbin/cron` if jobs do not run.
- Use the absolute venv Python path (as in the examples).
- For a laptop, a launchd plist or keeping the machine awake is more reliable than cron. macOS may sleep through `:00/:05` ticks.

### systemd timer alternative (Linux)

`/etc/systemd/system/binance-coralogix.service`:

```ini
[Unit]
Description=Ship Binance exchange logs to Coralogix

[Service]
Type=oneshot
WorkingDirectory=/opt/binance-coralogix
EnvironmentFile=/opt/binance-coralogix/.env
ExecStart=/opt/binance-coralogix/.venv/bin/python ship_binance_to_coralogix.py --once --lookback-minutes 5
```

`/etc/systemd/system/binance-coralogix.timer`:

```ini
[Unit]
Description=Run Binance → Coralogix every 5 minutes

[Timer]
OnCalendar=*:0/5
Persistent=true

[Install]
WantedBy=timers.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now binance-coralogix.timer
sudo systemctl list-timers | grep binance
```

`Persistent=true` runs a missed tick after the host comes back online. Combined with `state.json`, that still ships the gap.

## Finding logs in Coralogix

In **Explore** / **Logs**:

- Application: `binance` (or `CORALOGIX_APPLICATION_NAME`)
- Subsystem: `exchange-logs` (or `CORALOGIX_SUBSYSTEM_NAME`)
- Time picker must cover the Binance event timestamp (`insertTime`, `applyTime`, `time`, …)

Useful fields inside `text`:

- `event_source`: `binance_deposit`, `binance_withdraw`, `binance_transfer`, `binance_trade`, `binance_order`
- `cx_category`: `binance-deposit`, `binance-withdraw`, …
- Deposit / withdraw: `coin`, `amount`, `network`, `address`, `txId`, `status`
- Transfer: `asset`, `amount`, `tranId`, `transferType`
- Trade / order: `symbol`, `side`, `price`, `qty`, `status`

## CLI

```text
python ship_binance_to_coralogix.py --help

--once                  One cycle and exit (use with cron)
--dry-run               Fetch but do not send or save state
--lookback-minutes N    Override LOOKBACK_MINUTES (default 5)
--lookback-hours N      Convenience override
--log-level LEVEL       DEBUG, INFO, WARNING, ERROR
```

Cron and `run.sh` always use `--once --lookback-minutes 5`.

## Security

- Never commit `.env`. It is gitignored.
- Use a **read-only** Binance API key. Disable trade and withdraw.
- Restrict the key to the poller IP.
- Rotate Binance and Coralogix keys if they are exposed.

## Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| `-2015 Invalid API-key, IP, or permissions` | Wrong key/secret, Reading not enabled, or host IP not allowlisted. |
| `-1021 Timestamp outside recvWindow` | Host clock drift. Enable NTP, or raise `BINANCE_RECV_WINDOW` (max 60000). |
| `-1002 You are not authorized` | API key cannot read USER_DATA. Enable Reading. |
| Transfer type failed | That wallet product is not enabled on the account. Remove it from `BINANCE_TRANSFER_TYPES`. |
| No logs in Coralogix | Wrong `CORALOGIX_DOMAIN`, or Explore time range misses event timestamps. Empty Binance history also ships `0` events. |
| `Missing required environment variable` | `.env` missing in the cron working directory. |
| Cron runs with no output | Check absolute paths, venv Python, and log file permissions. |
| Job never fires | Cron daemon down, macOS sleep, or crontab installed for a different user. |
| Rate limited (`429` / `418`) | Poller retries using `Retry-After`. Reduce `BINANCE_TRANSFER_TYPES` or symbols if it persists. |

## License

Use and modify freely for your organization.
