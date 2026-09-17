# 代理池服务镜像
# 在镜像内安装依赖、编译 TypeScript，运行时不依赖宿主机环境（本机无需 npm install）。
# 基础镜像使用 README 指定的 Node v24.21.0（Alpine 精简版，DB 用 node:sqlite 无需原生编译）。
FROM node:24.21.0-alpine AS build

WORKDIR /srv

# 先只拷贝依赖清单，利用 Docker 层缓存加速重复构建
COPY services/proxies/package.json services/proxies/package.json
RUN cd services/proxies && npm install

# 拷贝源码并编译
COPY services/proxies services/proxies
RUN cd services/proxies && npm run build

# 删除编译期依赖，仅保留运行时依赖（proxy-agent）
RUN cd services/proxies && npm prune --omit=dev

# ---- 运行阶段 ----
FROM node:24.21.0-alpine

# 目录布局与源码推导一致：/srv（仓库根）下包含 services / data / logs / .env
WORKDIR /srv/services/proxies

# 从构建阶段拷贝编译产物、运行时依赖与数据源清单
COPY --from=build /srv/services/proxies/dist ./dist
COPY --from=build /srv/services/proxies/node_modules ./node_modules
COPY --from=build /srv/services/proxies/package.json ./package.json
COPY --from=build /srv/services/proxies/source.yaml ./source.yaml

# 数据与日志目录，运行时由宿主机卷覆盖（见 docker-compose.yaml）
RUN mkdir -p /srv/data/proxies /srv/logs/proxies

# 容器内端口（与 .env 中 PORT 一致，可覆盖）
EXPOSE 3000

CMD ["node", "--no-warnings", "dist/index.js"]