# Telegram Broker Bot — Webhook build (Render)

Uses Telegram **webhook** instead of polling (no 409 conflicts).

## Deploy on Render
1. Create a **Web Service**.
2. Set env vars:
   - `BOT_TOKEN` — from @BotFather
   - `ADMIN_IDS` — comma separated
   - `WEBHOOK_BASE` — your service URL, e.g. `https://<your-service>.onrender.com`
   - `WEBHOOK_PATH` — default `tg-webhook`
   - `WEBHOOK_SECRET` — random string (Render blueprint auto-generates)
   - `USE_POLLING=0`
3. Deploy.

The app binds to `$PORT` and sets the webhook to `WEBHOOK_BASE/WEBHOOK_PATH`.

### Local dev
`USE_POLLING=1` and run `python broker_bot.py`.

