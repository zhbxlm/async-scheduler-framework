<!-- 中文文档 -->
# Kubernetes 部署

Helm Chart 和 Kubernetes Manifest 将在后续版本提供。

当前阶段请使用 Docker Compose 进行本地和预发布环境的部署。

详见 [Docker 部署](docker-compose.md)。

## 规划功能

未来版本计划提供：

- Helm Chart（含 values.yaml 参数化配置）
- task-api / ops-api 独立 Deployment
- HPA 水平自动扩缩容
- ConfigMap + Secret 管理
- Ingress 配置示例（Nginx / Traefik）
- PodDisruptionBudget（PDB）
