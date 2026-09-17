/** 四协议常量。 */
export const PROTOCOLS = {
  HTTP: 1,
  HTTPS: 2,
  SOCKS4: 3,
  SOCKS5: 4,
} as const;

export type ProtocolType = (typeof PROTOCOLS)[keyof typeof PROTOCOLS];

/** 协议 type 数字 → 名字。 */
export const TYPE_TO_NAME: Record<number, string> = {
  [PROTOCOLS.HTTP]: 'http',
  [PROTOCOLS.HTTPS]: 'https',
  [PROTOCOLS.SOCKS4]: 'socks4',
  [PROTOCOLS.SOCKS5]: 'socks5',
};

/** 协议名字 → 数字。 */
export const NAME_TO_TYPE: Record<string, number> = {
  http: PROTOCOLS.HTTP,
  https: PROTOCOLS.HTTPS,
  socks4: PROTOCOLS.SOCKS4,
  socks5: PROTOCOLS.SOCKS5,
};

/** 全部协议类型，用于测活。 */
export const ALL_TYPES: ProtocolType[] = [PROTOCOLS.HTTP, PROTOCOLS.HTTPS, PROTOCOLS.SOCKS4, PROTOCOLS.SOCKS5];

/** 协议类型 → 代理连接使用的 URL scheme。 */
export function typeToScheme(type: number): string {
  switch (type) {
    case PROTOCOLS.HTTP:
      return 'http';
    case PROTOCOLS.HTTPS:
      return 'https';
    case PROTOCOLS.SOCKS4:
      return 'socks4';
    case PROTOCOLS.SOCKS5:
      return 'socks5';
    default:
      return 'http';
  }
}

export interface ProxyAddr {
  ip: string;
  port: number;
}

/** 数据库中的代理记录（用于测活调度）。 */
export interface ProxyRecord {
  ip: string;
  port: number;
  consecutiveFail: number;
}

/**
 * 单个协议的探测结果（对象，非 bool）。
 * ok 表示该协议当前可用；latencyMs 为成功时的延迟（毫秒）。
 */
export interface ProbeResult {
  ok: boolean;
  latencyMs: number | null;
}