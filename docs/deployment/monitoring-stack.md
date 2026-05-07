<!-- 中文文档 -->
# 监控栈部署

完整的监控配置说明请参见 [监控与告警配置](../guides/monitoring.md)。

## 快速启动

```bash
docker-compose -f docker-compose.monitoring.yml up -d
```

启动后访问 Grafana：<http://localhost:3000>（账号 `admin` / 密码 `admin`）。

## 监控栈组成

| 组件 | 端口 | 说明 |
|------|------|------|
| Prometheus | 9090 | 指标采集与存储 |
| Grafana | 3000 | 可视化仪表板 |
| Alertmanager | 9093 | 告警路由与通知 |

## 内置仪表板

启动后 Grafana 会自动加载以下仪表板：

- **队列健康度**：各 capability 的待处理 / 运行中任务数
- **任务吞吐量**：任务创建率、完成率、成功率
- **执行时长分布**：P50 / P95 / P99 时延（按 capability）
- **系统资源**：内存、CPU、Redis 连接数
