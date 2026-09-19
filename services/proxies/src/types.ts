/** 三种对外提供的代理协议常量。保留历史数字编号，避免现有 Redis 索引错位。 */
export const PROTOCOLS = {
  HTTPS: 2,
  SOCKS4: 3,
  SOCKS5: 4,
} as const;

export type ProtocolType = (typeof PROTOCOLS)[keyof typeof PROTOCOLS];

/** 协议 type 数字 → 名字。 */
export const TYPE_TO_NAME: Record<number, string> = {
  [PROTOCOLS.HTTPS]: 'https',
  [PROTOCOLS.SOCKS4]: 'socks4',
  [PROTOCOLS.SOCKS5]: 'socks5',
};

/** 协议名字 → 数字。 */
export const NAME_TO_TYPE: Record<string, number> = {
  https: PROTOCOLS.HTTPS,
  socks4: PROTOCOLS.SOCKS4,
  socks5: PROTOCOLS.SOCKS5,
};

/** 全部协议类型，用于测活。 */
export const ALL_TYPES: ProtocolType[] = [PROTOCOLS.HTTPS, PROTOCOLS.SOCKS4, PROTOCOLS.SOCKS5];

/** 协议类型 → 代理连接使用的 URL scheme。 */
export function typeToScheme(type: number): string {
  switch (type) {
    case PROTOCOLS.HTTPS:
      return 'https';
    case PROTOCOLS.SOCKS4:
      return 'socks4';
    case PROTOCOLS.SOCKS5:
      return 'socks5';
    default:
      throw new Error('不支持的代理协议类型: ' + type);
  }
}

export interface ProxyAddr {
  ip: string;
  port: number;
  /** 可选的认证用户名（源行含 username:password@ 时存在） */
  username?: string;
  /** 可选的认证密码 */
  password?: string;
}

/**
 * 构造 Redis 中使用的代理标识。
 * IPv6 使用方括号包裹，认证信息进行百分号编码，避免冒号造成字段歧义。
 */
export function formatAddrKey(proxy: ProxyAddr): string {
  const host = proxy.ip.includes(':') ? `[${proxy.ip}]` : proxy.ip;
  if (proxy.username === undefined) return `${host}:${proxy.port}`;
  return `${host}:${proxy.port}:${encodeURIComponent(proxy.username)}:${encodeURIComponent(proxy.password ?? '')}`;
}

/** 从代理标识中还原结构化地址。兼容历史 IPv4 标识。 */
export function parseAddrKey(addrKey: string): ProxyAddr | null {
  const match = addrKey.startsWith('[')
    ? /^\[([^\]]+)]:(\d+)(?::([^:]*):(.*))?$/.exec(addrKey)
    : /^([^:]+):(\d+)(?::([^:]*):(.*))?$/.exec(addrKey);
  if (!match) return null;

  const port = Number(match[2]);
  if (!Number.isInteger(port) || port < 1 || port > 65535) return null;

  const result: ProxyAddr = { ip: match[1], port };
  if (match[3] !== undefined) {
    try {
      result.username = decodeURIComponent(match[3]);
      result.password = decodeURIComponent(match[4] ?? '');
    } catch {
      return null;
    }
  }
  return result;
}

/**
 * 单个协议的探测结果（对象，非 bool）。
 * ok 表示该协议当前可用；latencyMs 为成功时的延迟（毫秒）。
 */
export interface ProbeResult {
  ok: boolean;
  latencyMs: number | null;
}
