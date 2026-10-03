#!/usr/bin/env node
/**
 * 内网穿透节点状态探测脚本
 * - 零第三方依赖，要求 Node.js >= 18
 * - 读取 nodes.json，更新（或创建）status.json
 * - 判定规则：每轮探测失败会自动重试；连续 downThreshold 轮失败才标红，恢复立即翻绿
 * - 可选告警：在仓库 Settings → Secrets 中配置 FEISHU_WEBHOOK，状态翻转时推送飞书群
 */
import { readFileSync, writeFileSync, existsSync } from 'node:fs';
import { connect } from 'node:net';
import http from 'node:http';
import https from 'node:https';
import { setTimeout as sleep } from 'node:timers/promises';

const ROOT = new URL('.', import.meta.url);
const config = JSON.parse(readFileSync(new URL('nodes.json', ROOT), 'utf8'));

const settings = {
  probeTimeoutMs: 5000,
  attempts: 2,
  retryDelayMs: 3000,
  downThreshold: 2,
  historyLimit: 2016, // 每 5 分钟一次时约保留 7 天
  ...(config.settings ?? {})
};

if (!Array.isArray(config.nodes) || config.nodes.length === 0) {
  console.error('nodes.json 中没有配置任何节点');
  process.exit(1);
}

const DAY_MS = 24 * 60 * 60 * 1000;

function probeTcp(host, port, timeoutMs) {
  return new Promise((resolve) => {
    const started = Date.now();
    let settled = false;
    const socket = connect({ host, port });
    socket.setTimeout(timeoutMs);

    function finish(up, error) {
      if (settled) return;
      settled = true;
      socket.destroy();
      resolve(up
        ? { up: true, latencyMs: Date.now() - started }
        : { up: false, latencyMs: null, error });
    }

    socket.once('connect', () => finish(true));
    socket.once('timeout', () => finish(false, 'timeout'));
    socket.once('error', (err) => finish(false, err.code || err.message));
  });
}

function probeHttp(host, port, useTls, path, timeoutMs) {
  return new Promise((resolve) => {
    const started = Date.now();
    let settled = false;

    function finish(up, error, statusCode) {
      if (settled) return;
      settled = true;
      try { req.destroy(); } catch {}
      resolve(up
        ? { up: true, latencyMs: Date.now() - started, statusCode }
        : { up: false, latencyMs: null, error });
    }

    const req = (useTls ? https : http).request({
      host,
      port,
      path: path || '/',
      method: 'GET',
      timeout: timeoutMs,
      rejectUnauthorized: false, // 自签证书也视为“服务活着”
      headers: { 'User-Agent': 'frp-status-monitor/1.0', Connection: 'close' }
    }, (res) => {
      res.resume();
      // 收到响应头即可判定服务存活，无需等待 body 结束
      finish(true, null, res.statusCode);
    });

    req.on('timeout', () => req.destroy(new Error('timeout')));
    req.on('error', (err) => finish(false, err.code || err.message));
    req.end();
  });
}

function probeOnce(node) {
  const { host, port } = node;
  switch (node.type) {
    case 'http':
      return probeHttp(host, port, false, node.path, settings.probeTimeoutMs);
    case 'https':
      return probeHttp(host, port, true, node.path, settings.probeTimeoutMs);
    case 'tcp':
    default:
      return probeTcp(host, port, settings.probeTimeoutMs);
  }
}

async function probeNode(node) {
  let result;
  for (let i = 1; i <= settings.attempts; i++) {
    result = await probeOnce(node);
    result.attempts = i;
    if (result.up || i === settings.attempts) return result;
    await sleep(settings.retryDelayMs);
  }
  return result;
}

function uptimeRatio(history, windowMs) {
  const cutoff = Date.now() - windowMs;
  const points = history.filter((p) => Date.parse(p.t) >= cutoff);
  if (points.length === 0) return null;
  return points.filter((p) => p.up).length / points.length;
}

async function notifyFeishu(events) {
  const webhook = process.env.FEISHU_WEBHOOK;
  if (!webhook || events.length === 0) return;
  const lines = events.map((e) =>
    `${e.to === 'down' ? '🔴' : '🟢'} ${e.name}（${e.host}:${e.port}）${e.to === 'down' ? '已中断' : '已恢复'}\n时间：${e.time}`);
  try {
    const res = await fetch(webhook, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        msg_type: 'text',
        content: { text: `【内网穿透节点状态通知】\n${lines.join('\n')}` }
      })
    });
    console.log('飞书通知已发送，HTTP', res.status);
  } catch (err) {
    console.warn('飞书通知发送失败:', err.message);
  }
}

// ---- 主流程 ----
const statusFileUrl = new URL('status.json', ROOT);
let prev = { updatedAt: null, nodes: [] };
if (existsSync(statusFileUrl)) {
  try {
    prev = JSON.parse(readFileSync(statusFileUrl, 'utf8'));
  } catch (err) {
    console.warn('旧 status.json 解析失败，将重建:', err.message);
  }
}
const prevById = new Map(prev.nodes.map((n) => [n.id, n]));

const now = new Date().toISOString();
const events = [];
const results = [];

for (const node of config.nodes) {
  if (!node.id || !node.host || !node.port) {
    console.warn('跳过配置不完整的节点:', JSON.stringify(node));
    continue;
  }

  const old = prevById.get(node.id) || {};
  const probe = await probeNode(node);

  const history = Array.isArray(old.history) ? old.history.slice() : [];
  history.push({ t: now, up: probe.up, latencyMs: probe.latencyMs ?? null });
  while (history.length > settings.historyLimit) history.shift();

  const consecutiveFailures = probe.up ? 0 : (old.consecutiveFailures || 0) + 1;
  const status = consecutiveFailures >= settings.downThreshold ? 'down' : 'up';
  const previousStatus = old.status || 'up';
  const lastChangeAt = status === previousStatus ? (old.lastChangeAt || now) : now;

  if (status !== previousStatus) {
    events.push({
      name: node.name || node.id,
      host: node.host,
      port: node.port,
      to: status,
      failures: consecutiveFailures,
      time: new Date().toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai', hour12: false })
    });
  }

  results.push({
    id: node.id,
    name: node.name || node.id,
    host: node.host,
    port: node.port,
    type: node.type || 'tcp',
    status,
    latencyMs: probe.latencyMs ?? null,
    consecutiveFailures,
    lastChangeAt,
    uptime24h: uptimeRatio(history, DAY_MS),
    uptime7d: uptimeRatio(history, 7 * DAY_MS),
    history
  });
}

writeFileSync(statusFileUrl, JSON.stringify({ updatedAt: now, nodes: results }, null, 2) + '\n');

for (const n of results) {
  console.log(
    `${n.status === 'up' ? '🟢' : '🔴'} ${n.name}  ${n.host}:${n.port}  ` +
    `${n.status.toUpperCase()}  latency=${n.latencyMs ?? '-'}ms  fails=${n.consecutiveFailures}`
  );
}

await notifyFeishu(events);
