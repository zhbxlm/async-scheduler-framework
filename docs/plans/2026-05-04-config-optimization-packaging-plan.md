# 2026-05-04 配置增强 / 调度优化 / 部署打包验证计划

## 目标

在不偏离 deepwiki 对齐主线的前提下，继续推进三件事：

1. 配置体系增强：让运行模式、部署角色、校验与摘要更统一，减少脚本和代码里的分散约定。
2. 调度框架优化：优先做低风险高收益的运行时与服务装配优化，不做大拆大改。
3. 部署打包链路验证：让本地 split 部署、Docker 打包、配置自检形成一条更清晰可验证的链路。

## 范围控制

本轮不做：
- deepwiki 平台级控制面的完整重构
- 大范围改 CLI 语义
- 侵入式改动 consumer / reconciler 主流程

## 任务拆分

### Task 1 配置体系增强
- 为 settings 增加 deployment / role / distributed_enabled / validation / summary 能力
- 测试覆盖默认值、显式 env、校验失败、分布式开关推断
- 保持现有 env 变量兼容

### Task 2 部署脚本与配置打通
- 新增配置自检入口
- `scripts/run_local_split.sh` 改为走统一环境变量约定
- 支持通过角色名注入 SERVICE_NAME / NODE_ID / DEPLOYMENT_ROLE
- 增加 dry-run / validate 级别验证

### Task 3 调度框架小步优化
- 优化 CLI 内对后端配置重复装配的地方
- 统一 backend config 构造路径，降低分散逻辑
- 补回归测试

### Task 4 部署打包链路验证
- 跑配置测试
- 跑 gap-fill / 关键回归测试
- 验证 Docker build 脚本和 split 脚本至少到可执行/可自检状态
- 输出结果到简明总结文档

## 验证标准
- settings/config 相关测试通过
- 关键 deepwiki gap-fill 测试通过
- split 脚本支持 validate/dry-run
- Docker build 命令可成功执行到镜像构建完成或至少明确暴露环境缺失

## 备注
- 本轮遵循“先测后改，尽量外科手术式修改”。
- 以提升工程可部署性和可解释性为主。