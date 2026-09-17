/**
 * 采集行的解析与地址合法性校验。
 * 源行形如 protocol://ip:port:country，protocol 或 country 极可能缺失，
 * 我们只使用 ip 与 port。
 * 地址合法性为纯规则判断：跳过注释、含非 ASCII（如中文）的行；端口必须在
 * 1~65535；ip 必须是公网单播地址（内网/私有/回环/链路本地/保留/组播/广播一律拒绝）。
 */
import type { ProxyAddr } from './types.js';

// 从行首提取的 ip 各段是否为合法 0~255 数值已由正则范围 + 数值判断处理。

// IPv4 私网/保留网段判断。命中任一则返回 false（拒绝）。
function isPublicIPv4(ip: string): boolean {
  const m = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/.exec(ip);
  if (!m) return false;
  const a = Number(m[1]);
  const b = Number(m[2]);
  const c = Number(m[3]);
  const d = Number(m[4]);
  if (a > 255 || b > 255 || c > 255 || d > 255) return false;
  if (a === 0) return false; // 0.0.0.0/8 本网络
  if (a === 10) return false; // 10.0.0.0/8 私有
  if (a === 100 && b >= 64 && b <= 127) return false; // 100.64.0.0/10 CGNAT
  if (a === 127) return false; // 127.0.0.0/8 回环
  if (a === 169 && b === 254) return false; // 169.254.0.0/16 链路本地
  if (a === 172 && b >= 16 && b <= 31) return false; // 172.16.0.0/12 私有
  if (a === 192 && b === 0) return false; // 192.0.0.0/24 保留
  if (a === 192 && b === 0 && c === 2) return false; // 192.0.2.0/24 TEST-NET
  if (a === 192 && b === 168) return false; // 192.168.0.0/16 私有
  if (a === 198 && b === 18) return false; // 198.18.0.0/15 基准测试
  if (a === 198 && b === 51 && c === 100) return false; // 198.51.100.0/24 TEST-NET
  if (a === 203 && b === 0 && c === 113) return false; // 203.0.113.0/24 TEST-NET
  if (a >= 224) return false; // 224.0.0.0/4 组播；240.0.0.0/4 保留；255.255.255.255 广播
  return true;
}

// IPv6：拒绝本地链路/唯一本地地址/回环/组播等非公网单播。不做完整 RFC 校验，
// 仅过滤明显的非公网地址，交给后续真实测活去判定。
function isPublicIPv6(ip: string): boolean {
  const m = /^[0-9a-fA-F:]+$/.exec(ip);
  if (!m || !ip.includes(':')) return false;
  const lower = ip.toLowerCase();
  if (lower === '::1') return false; // 回环
  if (/^fe[89ab]/i.test(lower)) return false; // 链路本地 fe80::/10、febc 等
  if (/^fc/i.test(lower) || /^fd/i.test(lower)) return false; // ULA fc00::/7
  if (/^ff/i.test(lower)) return false; // 组播 ff00::/8
  return true;
}

/** 判断 ip 是否为公网单播地址（IPv4 或 IPv6）。 */
export function isPublicIp(ip: string): boolean {
  return isPublicIPv4(ip) || isPublicIPv6(ip);
}

/**
 * 从一行原始文本解析出 (ip, port)。返回 null 表示不满足条件：
 *  - 空行、注释行、或含非 ASCII 字符（如中文）的行跳过
 *  - 端口必须在 1~65535
 *  - 原始行必须是可解析的 ip + port 形式
 *  - ip 必须是公网单播地址
 */
export function parseProxyLine(line: string): ProxyAddr | null {
  const t = line.trim();
  if (!t) return null;
  if (t.startsWith('#') || t.startsWith(';') || t.startsWith('//')) return null;
  // 含非 ASCII 字符（中文等）一律跳过
  if (!/^[\x00-\x7F]+$/.test(t)) return null;

  // 去掉可选的 protocol:// 前缀
  const schemeIdx = t.indexOf('://');
  const rest = schemeIdx === -1 ? t : t.slice(schemeIdx + 3);

  let ip: string | null = null;
  let portStr: string | null = null;

  // 形如 [ipv6]:port:country —— 支持带冒号的 IPv6 括号形式
  const bracket = /^\[([0-9a-fA-F:]+)\]:(\d+)/.exec(rest);
  if (bracket) {
    ip = bracket[1];
    portStr = bracket[2];
  } else {
    // 普通 IPv4 形式 ip:port[:country...]
    const plain = /^(?:\d{1,3}\.){3}\d{1,3}:(\d+)/.exec(rest);
    if (!plain) return null;
    ip = rest.slice(0, rest.indexOf(':'));
    portStr = plain[1];
  }

  if (ip === null || portStr === null) return null;
  const port = Number(portStr);
  if (!Number.isInteger(port) || port < 1 || port > 65535) return null;
  if (!isPublicIp(ip)) return null;

  return { ip, port };
}