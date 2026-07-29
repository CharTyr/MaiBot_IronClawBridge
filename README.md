# MaiBot IronClaw Bridge

将运行在 NEAR AI 上的 IronClaw 作为 MaiBot Planner 的异步远程执行器。

Planner 通过 `delegate_to_agent` 提交耗时任务；插件在后台轮询结果，完成后将任务快照与结果注入聊天上下文，再用 `maisaka.proactive.trigger` 唤醒 Planner 处理回复。

## 提供的 Tool

- `delegate_to_agent(title, task, background?)`：异步委派代码、深度检索、文件处理或多步任务。
- `ironclaw_status(task_id?)`：查询执行中或近期任务。
- `ironclaw_cancel(task_id)`：终止正在执行的任务。

## 运行架构

```text
MaiBot plugin (basechar)
  └─ SSH → NEAR AI baremetal3
       └─ nohup + setsid ironclaw run --cli-only --auto-approve
            └─ result file
  └─ poll result → context.append → proactive.trigger
```

## 必要条件

- basechar 到 IronClaw 主机的 SSH key 登录可用。
- 远端用户具备无密码 `sudo`；插件用它只读取常驻 IronClaw 进程的运行环境。
- 远端常驻 IronClaw 进程以 `ironclaw run --no-onboard` 运行。
- MaiBot rdev runtime 已提供 `maisaka.context.append`、`maisaka.proactive.trigger` 与 `message.get_recent` capability。

## 关键运行约束

### 容器重启无需人工修复

SSH 登录 shell 不继承 NEAR AI 容器启动时注入的 `NEARAI_API_KEY`、`SECRETS_MASTER_KEY`、`LIBSQL_PATH` 等变量。

每次任务提交前，插件都会：

1. 通过 `sudo` 找到常驻 `ironclaw run --no-onboard` 进程；
2. 从其 root 父进程的 `/proc/<pid>/environ` 重新生成 `/home/agent/.ironclaw/runtime-env.sh`；
3. 再启动本次 CLI 任务。

`/home/agent` 是独立持久化挂载，因此容器重启后插件本身不需要额外部署或手工刷新。环境文件会在下一次任务派遣时自动重建。

### 仅支持串行任务

当前 IronClaw CLI 实例共享 libSQL 数据库、工作目录和 PID 锁。`max_concurrent` 固定有效值为 `1`；设置更大值会被插件忽略并写警告日志。

`--no-db` 虽能绕开数据库竞争，但会让 workspace/routines 退回内存态，不能用来伪造并发能力。

### 自动批准

默认 `auto_approve = true`，任务进程携带 `--auto-approve`。它自动批准常规 shell、文件和 HTTP 工具调用；IronClaw CLI 仍会保留其破坏性操作、认证、hooks 和限速保护。

### 终止与超时

每个任务在独立 `setsid` 进程组中执行。取消或超时时，插件会终止整组进程；超时默认 600 秒。结果文件只保留在 NEAR AI 主机，不占 basechar 磁盘。

## 配置

运行时配置路径：

```text
data/plugins/chartyr.ironclaw-bridge/config.toml
```

源文件中的 `config.toml` 可作为初始模板。修改后由 MaiBot 插件配置热更新机制加载。

## 安装

```bash
git clone https://github.com/CharTyr/MaiBot_IronClawBridge.git \
  /root/seren/rdev-Maibot/plugins/CharTyr_IronClaw_Bridge
mkdir -p /root/seren/rdev-Maibot/data/plugins/chartyr.ironclaw-bridge
cp /root/seren/rdev-Maibot/plugins/CharTyr_IronClaw_Bridge/config.toml \
  /root/seren/rdev-Maibot/data/plugins/chartyr.ironclaw-bridge/config.toml
```

MaiBot 重启由部署者自行执行。插件代码热更新后，先确认 plugin runner 的加载日志。
