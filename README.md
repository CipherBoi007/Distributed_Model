# Distributed Model — 3-node Deployment

This repo demonstrates a simple distributed system you can run on 3 machines.

Quick steps to deploy on each machine:

1. Copy the repository to each machine.
2. Install dependencies (recommended: use the included `env` virtualenv or create a new one):

```powershell
python -m venv env
env\Scripts\Activate.ps1
pip install -r requirements.txt
```

3. Configure per-machine environment variables (either export them or create a `.env` file from `.env.example`):

- `NODE_ID` — node identifier (1, 2 or 3).
- `NODE_IP` — machine IP address to bind the server to.
- `NODE_PORT` — port to run the server on.
- `OPENROUTER_API_KEY` — your OpenRouter API key.

Note: the application will automatically load a `.env` file if present (uses `python-dotenv`).

You can also edit `config.yaml` to list the three nodes. Use `${NODE_IP}` placeholders if you prefer to keep `config.yaml` generic, for example:

```yaml
nodes:
  - id: 1
    ip: "${NODE1_IP}"
    port: ${NODE1_PORT}
  - id: 2
    ip: "${NODE2_IP}"
    port: ${NODE2_PORT}
  - id: 3
    ip: "${NODE3_IP}"
    port: ${NODE3_PORT}

api:
  openrouter_api_key: "${OPENROUTER_API_KEY}"
  openrouter_base_url: "https://openrouter.ai/api/v1"
  model: "mistralai/mixtral-8x7b-instruct"
```

4. Start the node (from repo root):

```powershell
# set NODE_ID and NODE_IP/NODE_PORT per machine or pass overrides
python run.py --node-id 1
# or with override
python run.py --node-id 1 --ip 192.168.1.100 --port 8000
```

Notes
- `run.py` will expand `${VARNAME}` placeholders found in `config.yaml` from environment variables.
- The app will try to read `OPENROUTER_API_KEY` from the config or environment.
- Logs are written to `logs/node_<NODE_ID>_<NODE_IP>.log`.

If you'd like, I can also:
- Add a small systemd/service file for Linux.
- Add a `.env` loader (python-dotenv) and automatic `.env` reading.
