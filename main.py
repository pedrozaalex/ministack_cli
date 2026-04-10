"""Ministack setup CLI for the payment flow simulation.

Flow:
  payment-create (Function URL)
    -> SQS payment-consumer-queue
      -> payment-consumer Lambda
        -> SQS scheduler-queue
          -> scheduler Lambda
            -> HTTP PATCH payment-create /jobs/execute

Uses ministack (free, MIT-licensed AWS emulator) instead of LocalStack.
All .NET Lambdas use the dotnet8 runtime via Docker RIE.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, Future
from dataclasses import dataclass, field
from pathlib import Path

from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.spinner import Spinner


# ── Globals ──────────────────────────────────────────────────────────────────

console = Console()
ENDPOINT = "http://localhost:4566"
CONTAINER = "ministack"
IMAGE = "nahuelnucera/ministack"
REGION = "us-east-1"
REPOS_ROOT = Path("/mnt/c/repos")

ALREADY_EXISTS = ["ResourceInUseException", "QueueAlreadyExists", "ResourceConflictException"]


# ── Helpers ──────────────────────────────────────────────────────────────────

def sh(cmd: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, capture_output=True, text=True, check=check)


def aws(*args: str) -> subprocess.CompletedProcess[str]:
    return sh(["aws", "--endpoint-url", ENDPOINT, "--region", REGION, *args], check=False)


def is_exists(r: subprocess.CompletedProcess[str]) -> bool:
    return any(m in (r.stderr + r.stdout) for m in ALREADY_EXISTS)


def parse_dotenv(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    if not path.exists():
        return env
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key, value = key.strip(), value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
            value = value[1:-1]
        env[key] = value
    return env


def rewrite_for_docker(env: dict[str, str]) -> dict[str, str]:
    """Rewrite env values for Lambda Docker container context.
    Services on WSL host (Redis, PostgreSQL, BFF) need host.docker.internal.
    """
    HOST_KEYS = {"CACHE_REDIS_CONNECTIONSTRING", "CONNECTIONSTRINGS_POSTGRESS_HOST",
                 "CONNECTIONSTRING_POSTGRESS_HOST"}
    URL_KEYS = {"BFF_BASEURL"}
    result: dict[str, str] = {}
    for key, value in env.items():
        if not value:
            result[key] = value
        elif key in HOST_KEYS:
            result[key] = re.sub(r'^(localhost|127\.0\.0\.1)(?=[:/]|$)', 'host.docker.internal', value)
        elif key in URL_KEYS:
            result[key] = re.sub(r'(?<=://)(localhost|127\.0\.0\.1)(?=[:/]|$)', 'host.docker.internal', value)
        else:
            result[key] = value.replace("\\", "/") if "\\" in value else value
    return result


# ── Resource config ──────────────────────────────────────────────────────────

@dataclass(frozen=True)
class KeySchema:
    name: str
    type: str = "S"
    key_type: str = "HASH"

@dataclass(frozen=True)
class GSI:
    name: str
    hash_key: KeySchema
    range_key: KeySchema

@dataclass(frozen=True)
class DynamoTable:
    name: str
    keys: list[KeySchema]
    gsis: list[GSI] = field(default_factory=list)

@dataclass(frozen=True)
class SQSQueue:
    name: str
    @property
    def url(self) -> str:
        return f"{ENDPOINT}/000000000000/{self.name}"
    @property
    def arn(self) -> str:
        return f"arn:aws:sqs:{REGION}:000000000000:{self.name}"

@dataclass(frozen=True)
class LambdaFunction:
    name: str
    handler: str
    csproj: str  # Windows path for dotnet.exe
    publish_dir: str
    env_file: Path
    env_overrides: dict[str, str]
    timeout: int = 30
    memory: int = 512
    has_function_url: bool = False

    @property
    def zip_path(self) -> Path:
        return Path(f"/tmp/{self.publish_dir.removesuffix('-publish')}.zip")

    def resolve_env(self) -> dict[str, str]:
        env = parse_dotenv(self.env_file)
        env = rewrite_for_docker(env)
        env.update(self.env_overrides)
        return {k: v for k, v in env.items() if v}


# ── Resources ────────────────────────────────────────────────────────────────

QUEUES = [SQSQueue("payment-consumer-queue"), SQSQueue("scheduler-queue")]

TABLES = [
    DynamoTable("payment", keys=[
        KeySchema("id", "S", "HASH"), KeySchema("chargeId", "S", "RANGE"),
    ], gsis=[GSI("clientId-situation-index",
                 hash_key=KeySchema("clientId", "S", "HASH"),
                 range_key=KeySchema("situation", "S", "RANGE"))]),
    DynamoTable("PaymentCreateCardToken", keys=[KeySchema("cardToken", "S", "HASH")]),
    DynamoTable("credit_card", keys=[KeySchema("id", "S", "HASH")]),
    DynamoTable("audit", keys=[KeySchema("id", "S", "HASH")]),
    DynamoTable("SankyaOrder", keys=[
        KeySchema("id", "S", "HASH"), KeySchema("sk", "S", "RANGE"),
    ], gsis=[GSI("status-entityType-index",
                 hash_key=KeySchema("status", "S", "HASH"),
                 range_key=KeySchema("entityType", "S", "RANGE"))]),
]

CQ, SQ = QUEUES  # consumer queue, scheduler queue

# Placeholder tokens — replaced at deploy time with real Function URLs
_PH_PAYMENT_CREATE = "__PAYMENT_CREATE_URL__"
_PH_SECURITY_CUSTOMER = "__SECURITY_CUSTOMER_URL__"

LAMBDAS = [
    # ── Function URL services (deployed first so URLs are known) ──
    LambdaFunction(
        name="security-customer",
        handler="Soollar.SecurityCustomer.Backend.API",
        csproj="C:/repos/security_customer_main/src/Soollar.SecurityCustomer.Backend/Soollar.SecurityCustomer.Backend.API.csproj",
        publish_dir="security-customer-publish",
        env_file=REPOS_ROOT / "security_customer_main" / "src" / ".env",
        has_function_url=True,
        env_overrides={},
    ),
    LambdaFunction(
        name="payments-create",
        handler="Soollar.Payments.Api",
        csproj="C:/repos/payment-create/src/Soollar.Payments.Api/Soollar.Payments.Api.csproj",
        publish_dir="payment-create-publish",
        env_file=REPOS_ROOT / "payment-create" / ".env",
        timeout=60, memory=2048,
        has_function_url=True,
        env_overrides={
            "SQS_SERVICE_URL": "http://localhost.localstack.cloud:4566",
            "SQS_QUEUENAME": CQ.url,
            "SQS_REGION": REGION,
            "DYNAMODB_SERVICE_URL": "http://localhost.localstack.cloud:4566",
            "SECURITYCUSTOMER_BASEURL": _PH_SECURITY_CUSTOMER,
            "APPLICATION_NAME": "local",
            "APPLICATION_KEY": "local",
        },
    ),
    # ── SQS-triggered Lambdas ──
    LambdaFunction(
        name="payments-consumer",
        handler="Sollar.Payments.Consumer.Lambda::Sollar.Payments.Consumer.Lambda.Function::Handler",
        csproj="C:/repos/payment-consumer/src/Soollar.Payments.Consumer/Sollar.Payments.Consumer.Lambda/Sollar.Payments.Consumer.Lambda.csproj",
        publish_dir="payment-consumer-publish",
        env_file=REPOS_ROOT / "payment-consumer" / ".env",
        env_overrides={
            "SQS_QUEUE_QUEUENAME": CQ.url,
            "SQS_QUEUE_WEBHOOK_QUEUENAME": SQ.url,
            "SQS_REGION": REGION,
            "PAYMENTS_CREATE_BASE_URL": _PH_PAYMENT_CREATE,
        },
    ),
    LambdaFunction(
        name="scheduler",
        handler="Sollar.Scheduler.Lambda::Sollar.Scheduler.Lambda.Function::Handler",
        csproj="C:/repos/scheduler/src/Soollar.Scheduler/Sollar.Scheduler.Lambda/Sollar.Scheduler.Lambda.csproj",
        publish_dir="scheduler-publish",
        env_file=REPOS_ROOT / "scheduler" / "src" / "Soollar.Scheduler" / ".env",
        timeout=45,
        env_overrides={"SQS_RETRYQUEUE_QUEUENAME": SQ.url},
    ),
]

# SQS -> Lambda event source mappings
ESM_PAIRS = [(CQ, LAMBDAS[2]), (SQ, LAMBDAS[3])]


# ── Result tracking ─────────────────────────────────────────────────────────

@dataclass
class StepResult:
    name: str
    status: str
    detail: str = ""

    def print(self) -> None:
        style = {"created": "green", "exists": "yellow", "deployed": "green",
                 "failed": "red", "skipped": "dim"}.get(self.status, "white")
        detail = f"  [dim]{self.detail}[/dim]" if self.detail else ""
        console.print(f"  [{style}][{self.status:>9s}][/{style}]  {self.name}{detail}")


def _live_table(states: dict[str, StepResult], label: str) -> Table:
    t = Table(show_header=False, box=None, padding=(0, 1))
    for name, sr in states.items():
        if sr.status in ("building", "deploying"):
            t.add_row(Spinner("dots", style="cyan"), name, Text(f"{label}...", style="cyan"))
        else:
            style = "green" if sr.status in ("created", "deployed") else "red"
            t.add_row(Text(f"  [{sr.status}]", style=style), name, Text(sr.detail, style="dim"))
    return t


# ── Steps ────────────────────────────────────────────────────────────────────

def step_start_ministack() -> None:
    """Ensure ministack container is running."""
    with console.status("[cyan]Checking ministack...", spinner="dots"):
        r = sh(["docker", "inspect", "-f", "{{.State.Running}}", CONTAINER], check=False)

    if r.returncode == 0 and r.stdout.strip() == "true":
        console.print(f"  [green]ready[/]  {CONTAINER}")
        return

    console.print(f"  [yellow]starting[/]  {CONTAINER}")
    # Remove stopped container and check for port conflicts
    sh(["docker", "rm", "-f", CONTAINER], check=False)
    port_check = sh(["docker", "ps", "--filter", "publish=4566", "--format", "{{.Names}}"], check=False)
    if port_check.stdout.strip():
        blocker = port_check.stdout.strip()
        console.print(f"  [yellow]stopping[/]  {blocker} (port 4566 conflict)")
        sh(["docker", "rm", "-f", blocker], check=False)

    with console.status("[cyan]Starting ministack...", spinner="dots"):
        sh([
            "docker", "run", "-d",
            "--name", CONTAINER,
            "-p", "4566:4566",
            "-v", "/var/run/docker.sock:/var/run/docker.sock",
            "-e", "LAMBDA_EXECUTOR=docker",
            "-e", f"LAMBDA_DOCKER_NETWORK=bridge",
            "-e", "LAMBDA_DOCKER_FLAGS=--add-host=host.docker.internal:host-gateway",
            "-e", f"MINISTACK_REGION={REGION}",
            "-e", "MINISTACK_HOST=localhost.localstack.cloud",
            "-e", "LOG_LEVEL=INFO",
            IMAGE,
        ])
        # Wait for readiness
        for _ in range(30):
            time.sleep(1)
            r = sh(["curl", "-sf", f"{ENDPOINT}/_ministack/health"], check=False)
            if r.returncode != 0:
                r = sh(["curl", "-sf", f"{ENDPOINT}/health"], check=False)
            if r.returncode == 0:
                break
        else:
            console.print("  [red bold]ERROR[/]  ministack did not start in 30s")
            sys.exit(1)

    console.print(f"  [green]started[/]  {CONTAINER}")


def step_create_queues() -> list[StepResult]:
    results: list[StepResult] = []
    for q in QUEUES:
        r = aws("sqs", "create-queue", "--queue-name", q.name)
        if r.returncode == 0:
            results.append(StepResult(q.name, "created", q.url))
        elif is_exists(r):
            results.append(StepResult(q.name, "exists"))
        else:
            results.append(StepResult(q.name, "failed", r.stderr.strip()[:120]))
    return results


def step_create_tables() -> list[StepResult]:
    results: list[StepResult] = []
    for t in TABLES:
        attrs: dict[str, str] = {}
        for k in t.keys:
            attrs[k.name] = k.type
        for g in t.gsis:
            attrs[g.hash_key.name] = g.hash_key.type
            attrs[g.range_key.name] = g.range_key.type

        cmd = [
            "dynamodb", "create-table", "--table-name", t.name,
            "--attribute-definitions",
            *[f"AttributeName={n},AttributeType={tp}" for n, tp in attrs.items()],
            "--key-schema",
            *[f"AttributeName={k.name},KeyType={k.key_type}" for k in t.keys],
            "--billing-mode", "PAY_PER_REQUEST",
        ]
        if t.gsis:
            cmd += ["--global-secondary-indexes", json.dumps([{
                "IndexName": g.name,
                "KeySchema": [
                    {"AttributeName": g.hash_key.name, "KeyType": g.hash_key.key_type},
                    {"AttributeName": g.range_key.name, "KeyType": g.range_key.key_type},
                ],
                "Projection": {"ProjectionType": "ALL"},
            } for g in t.gsis])]

        r = aws(*cmd)
        if r.returncode == 0:
            keys = " + ".join(k.name for k in t.keys)
            gsi = f", GSI: {', '.join(g.name for g in t.gsis)}" if t.gsis else ""
            results.append(StepResult(t.name, "created", f"keys=[{keys}]{gsi}"))
        elif is_exists(r):
            results.append(StepResult(t.name, "exists"))
        else:
            results.append(StepResult(t.name, "failed", r.stderr.strip()[:120]))
    return results


def _build_one(fn: LambdaFunction, publish_root: Path) -> StepResult:
    out_win = f"C:/repos/Monorepo/.ministack-publish/{fn.publish_dir}"
    r = sh([
        "dotnet.exe", "publish", fn.csproj,
        "-c", "Release", "-r", "linux-x64", "--self-contained", "false",
        "-o", out_win, "-v", "quiet",
    ], check=False)
    if r.returncode != 0:
        return StepResult(fn.name, "failed", r.stderr.strip()[:120])

    src_dir = publish_root / fn.publish_dir
    file_count = 0
    with zipfile.ZipFile(fn.zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(src_dir):
            for f in files:
                fp = Path(root) / f
                zf.write(fp, fp.relative_to(src_dir))
                file_count += 1

    size_mb = fn.zip_path.stat().st_size / 1024 / 1024
    return StepResult(fn.name, "created", f"{size_mb:.1f} MB, {file_count} files")


def step_build_lambdas(publish_root: Path) -> list[StepResult]:
    states = {fn.name: StepResult(fn.name, "building") for fn in LAMBDAS}
    futures: dict[str, Future[StepResult]] = {}

    with ThreadPoolExecutor(max_workers=2) as pool:
        for fn in LAMBDAS:
            futures[fn.name] = pool.submit(_build_one, fn, publish_root)

        with Live(_live_table(states, "building"), console=console, refresh_per_second=8) as live:
            while not all(f.done() for f in futures.values()):
                for name, fut in futures.items():
                    if fut.done() and states[name].status == "building":
                        states[name] = fut.result()
                live.update(_live_table(states, "building"))
                time.sleep(0.1)
            for name, fut in futures.items():
                states[name] = fut.result()
            live.update(_live_table(states, "building"))

    return list(states.values())


_function_urls: dict[str, str] = {}


def _deploy_one(fn: LambdaFunction) -> StepResult:
    env = fn.resolve_env()

    # Resolve Function URL placeholders
    ph_map = {_PH_PAYMENT_CREATE: "payments-create", _PH_SECURITY_CUSTOMER: "security-customer"}
    for k, v in list(env.items()):
        if v in ph_map and ph_map[v] in _function_urls:
            env[k] = _function_urls[ph_map[v]]

    # Rewrite localhost:4566 → localhost.localstack.cloud:4566 for SQS/DynamoDB URLs.
    # The extra_hosts in the Lambda container maps localhost.localstack.cloud to ministack's IP.
    for k, v in list(env.items()):
        if v == ENDPOINT:
            env[k] = "http://localhost.localstack.cloud:4566"

    # Delete existing
    aws("lambda", "delete-function", "--function-name", fn.name)

    env_json = json.dumps({"Variables": env})
    r = aws(
        "lambda", "create-function",
        "--function-name", fn.name,
        "--runtime", "dotnet8",
        "--handler", fn.handler,
        "--zip-file", f"fileb://{fn.zip_path}",
        "--role", "arn:aws:iam::000000000000:role/lambda-role",
        "--timeout", str(fn.timeout),
        "--memory-size", str(fn.memory),
        "--environment", env_json,
    )
    if r.returncode != 0:
        return StepResult(fn.name, "failed", r.stderr.strip()[:120])

    data = json.loads(r.stdout)
    detail = f"timeout={data['Timeout']}s, memory={data['MemorySize']}MB"

    # Create Function URL for HTTP-accessible Lambdas
    if fn.has_function_url:
        aws("lambda", "create-function-url-config",
            "--function-name", fn.name, "--auth-type", "NONE")
        url_r = aws("lambda", "get-function-url-config", "--function-name", fn.name)
        if url_r.returncode == 0:
            func_url = json.loads(url_r.stdout).get("FunctionUrl", "").rstrip("/")
            _function_urls[fn.name] = func_url
            detail += f", url={func_url}"

    return StepResult(fn.name, "deployed", detail)


def step_deploy_lambdas() -> list[StepResult]:
    states = {fn.name: StepResult(fn.name, "deploying") for fn in LAMBDAS}
    with Live(_live_table(states, "deploying"), console=console, refresh_per_second=8) as live:
        for fn in LAMBDAS:
            states[fn.name] = _deploy_one(fn)
            live.update(_live_table(states, "deploying"))
    return list(states.values())


def step_create_mappings() -> list[StepResult]:
    results: list[StepResult] = []
    for queue, fn in ESM_PAIRS:
        r = aws(
            "lambda", "create-event-source-mapping",
            "--function-name", fn.name,
            "--event-source-arn", queue.arn,
        )
        label = f"{queue.name} -> {fn.name}"
        if r.returncode == 0:
            data = json.loads(r.stdout)
            results.append(StepResult(label, "created", f"batch_size={data.get('BatchSize', '?')}"))
        elif is_exists(r):
            results.append(StepResult(label, "exists"))
        else:
            results.append(StepResult(label, "failed", r.stderr.strip()[:120]))
    return results


def show_env_table(fn: LambdaFunction) -> None:
    env = fn.resolve_env()
    table = Table(title=fn.name, show_lines=False, title_style="bold cyan")
    table.add_column("Variable", style="cyan", no_wrap=True)
    table.add_column("Value")
    table.add_column("", width=10)
    for k, v in env.items():
        tag = Text("override", style="yellow") if k in fn.env_overrides else Text("")
        table.add_row(k, v[:77] + "..." if len(v) > 80 else v, tag)
    console.print(table)


def show_summary() -> None:
    with console.status("[cyan]Fetching status...", spinner="dots"):
        tables = aws("dynamodb", "list-tables", "--query", "TableNames", "--output", "text")
        funcs = aws("lambda", "list-functions", "--query", "Functions[*].FunctionName", "--output", "text")
        queues = aws("sqs", "list-queues", "--query", "QueueUrls", "--output", "text")
    console.print()
    console.print(f"  [bold]DynamoDB[/]  {tables.stdout.strip()}")
    console.print(f"  [bold]Lambda[/]    {funcs.stdout.strip()}")
    console.print(f"  [bold]SQS[/]       {queues.stdout.strip()}")


# ── Section helpers ──────────────────────────────────────────────────────────

def run_section(title: str, fn, *args, spinner_msg: str | None = None) -> list[StepResult]:
    console.print()
    console.rule(f"[bold]{title}", style="blue")
    if spinner_msg:
        with console.status(f"[cyan]{spinner_msg}", spinner="dots"):
            results = fn(*args)
    else:
        results = fn(*args)
    for r in results:
        r.print()
    return results


def run_live_section(title: str, fn, *args) -> list[StepResult]:
    console.print()
    console.rule(f"[bold]{title}", style="blue")
    return fn(*args)


# ── Main ─────────────────────────────────────────────────────────────────────

def cmd_deploy(args: argparse.Namespace) -> None:
    publish_root = Path(__file__).resolve().parent.parent.parent / ".ministack"
    start = time.monotonic()
    had_failures = False

    console.print()
    console.print(Panel.fit(
        "[bold]Ministack Setup[/bold]\n"
        "[dim]payment-create -> SQS -> consumer Lambda -> SQS -> scheduler Lambda[/dim]",
        border_style="blue",
    ))

    # ── Preflight: start ministack if needed ──
    console.print()
    console.rule("[bold]Preflight", style="blue")
    step_start_ministack()

    run_all = args.only is None

    if run_all or args.only == "queues":
        results = run_section("SQS Queues", step_create_queues, spinner_msg="Creating queues...")
        had_failures |= any(r.status == "failed" for r in results)

    if run_all or args.only == "tables":
        results = run_section("DynamoDB Tables", step_create_tables, spinner_msg="Creating tables...")
        had_failures |= any(r.status == "failed" for r in results)

    if run_all or args.only == "lambdas":
        if args.skip_build:
            console.print()
            console.rule("[bold]Build Lambda Packages", style="blue")
            console.print("  [yellow]skipped[/yellow]  --skip-build flag set")
        else:
            results = run_live_section("Build Lambda Packages", step_build_lambdas, publish_root)
            had_failures |= any(r.status == "failed" for r in results)

        results = run_live_section("Deploy Lambda Functions", step_deploy_lambdas)
        had_failures |= any(r.status == "failed" for r in results)

        if args.verbose:
            console.print()
            for fn in LAMBDAS:
                show_env_table(fn)

        results = run_section("Event Source Mappings", step_create_mappings,
                              spinner_msg="Wiring SQS -> Lambda...")
        had_failures |= any(r.status == "failed" for r in results)

    if args.only == "mappings":
        results = run_section("Event Source Mappings", step_create_mappings,
                              spinner_msg="Wiring SQS -> Lambda...")
        had_failures |= any(r.status == "failed" for r in results)

    show_summary()
    elapsed = time.monotonic() - start
    style = "red bold" if had_failures else "green bold"
    console.print()
    console.print(Panel.fit(f"[{style}]Completed in {elapsed:.1f}s[/]",
                            border_style="red" if had_failures else "green"))


def cmd_stop() -> None:
    sh(["docker", "rm", "-f", CONTAINER], check=False)
    console.print(f"  [green]stopped[/]  {CONTAINER}")


def cmd_status() -> None:
    r = sh(["docker", "inspect", "-f", "{{.State.Running}}", CONTAINER], check=False)
    if r.returncode != 0 or r.stdout.strip() != "true":
        console.print(f"  [red]not running[/]  {CONTAINER}")
        return
    console.print(f"  [green]running[/]  {CONTAINER}")
    show_summary()


def cmd_logs(args: argparse.Namespace) -> None:
    """Invoke a Lambda with a minimal event and display its logs, or show last invocation logs."""
    fn_name = args.function
    # Validate function exists
    r = aws("lambda", "get-function", "--function-name", fn_name)
    if r.returncode != 0:
        console.print(f"[red]Function '{fn_name}' not found[/red]")
        return

    if args.invoke:
        # Invoke with a test event and capture logs
        import base64 as b64
        payload = args.payload or '{}'
        console.print(f"[cyan]Invoking {fn_name}...[/cyan]")
        r = aws(
            "lambda", "invoke",
            "--function-name", fn_name,
            "--payload", payload,
            "--cli-binary-format", "raw-in-base64-out",
            "--log-type", "Tail",
            "/tmp/mstk-invoke-out.json",
        )
        if r.returncode != 0:
            console.print(f"[red]Invoke failed:[/red] {r.stderr.strip()}")
            return
        try:
            data = json.loads(r.stdout)
            # Print status
            status = data.get("StatusCode", "?")
            has_error = "FunctionError" in data
            style = "red" if has_error else "green"
            console.print(f"  [{style}]Status: {status}{'  FunctionError: ' + data['FunctionError'] if has_error else ''}[/{style}]")
            # Print logs
            if "LogResult" in data:
                logs = b64.b64decode(data["LogResult"]).decode("utf-8", errors="replace")
                console.print()
                console.rule("[bold]Lambda Logs", style="blue")
                for line in logs.strip().splitlines():
                    # Color code log lines
                    if "ERROR" in line or "error" in line or "Exception" in line:
                        console.print(f"  [red]{line}[/red]")
                    elif "WARN" in line or "warn" in line:
                        console.print(f"  [yellow]{line}[/yellow]")
                    elif line.startswith("START") or line.startswith("END") or line.startswith("REPORT"):
                        console.print(f"  [dim]{line}[/dim]")
                    else:
                        console.print(f"  {line}")
            # Print response
            console.print()
            console.rule("[bold]Response", style="blue")
            try:
                with open("/tmp/mstk-invoke-out.json") as f:
                    resp = json.load(f)
                console.print_json(json.dumps(resp, indent=2, ensure_ascii=False))
            except Exception:
                console.print(f"  [dim](no response body)[/dim]")
        except json.JSONDecodeError:
            console.print(r.stdout)
    else:
        # Show ministack container logs filtered for this function
        console.print(f"[cyan]Showing ministack logs for '{fn_name}'...[/cyan]")
        r = sh(["docker", "logs", CONTAINER, "--tail", "200"], check=False)
        logs = (r.stdout or "") + (r.stderr or "")
        found = False
        for line in logs.splitlines():
            if fn_name in line or "lambda" in line.lower():
                console.print(f"  {line}")
                found = True
        if not found:
            console.print(f"  [dim]No logs found for '{fn_name}'[/dim]")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ministack setup CLI for the payment flow simulation.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  mstk-setup deploy                  Full setup (starts ministack if needed)
  mstk-setup deploy --skip-build     Redeploy using last build
  mstk-setup deploy --only lambdas   Only rebuild and redeploy Lambdas
  mstk-setup logs payments-create    Show ministack logs for a function
  mstk-setup logs -i payments-create Invoke and show logs
  mstk-setup logs -i scheduler -p '{"Records":[...]}'
  mstk-setup stop                    Stop ministack container
  mstk-setup status                  Show ministack status
        """,
    )
    sub = parser.add_subparsers(dest="command")

    deploy_p = sub.add_parser("deploy", help="Deploy all resources (starts ministack if needed)")
    deploy_p.add_argument("--skip-build", action="store_true", help="Reuse existing zips")
    deploy_p.add_argument("--only", choices=["queues", "tables", "lambdas", "mappings"])
    deploy_p.add_argument("-v", "--verbose", action="store_true", help="Show resolved env vars")

    logs_p = sub.add_parser("logs", help="Show Lambda logs")
    logs_p.add_argument("function", help="Lambda function name")
    logs_p.add_argument("-i", "--invoke", action="store_true", help="Invoke the function and show logs")
    logs_p.add_argument("-p", "--payload", help="JSON payload for invoke (default: '{}')")

    sub.add_parser("stop", help="Stop ministack container")
    sub.add_parser("status", help="Show ministack status and resources")

    args = parser.parse_args()

    if args.command == "deploy" or args.command is None:
        if args.command is None:
            args.skip_build = False
            args.only = None
            args.verbose = False
        cmd_deploy(args)
    elif args.command == "logs":
        cmd_logs(args)
    elif args.command == "stop":
        cmd_stop()
    elif args.command == "status":
        cmd_status()


if __name__ == "__main__":
    main()
