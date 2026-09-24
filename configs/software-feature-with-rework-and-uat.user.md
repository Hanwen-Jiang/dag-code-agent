# OAuth 回调 state 校验交付

- 目标：实现可靠的 OAuth state 校验，并由内部技术验收和用户体验验收共同确认
- 版本：2
- 状态：COMPLETED
- 规格哈希：`sha256:3f60f30d95f6af2b524990a35e22729ec0f6ab830991e7694d37531490fdf0f0`

## 工作节点

| 节点 | 工作 | 负责人 | 依赖 | 用户参与 |
|---|---|---|---|---|
| `N-DESIGN` | 冻结 OAuth state 校验接口与错误契约 | `ARCHITECT` | 无 | 否 |
| `N-IMPLEMENT-V1` | 按冻结契约实现第一版 state 校验 | `DEVELOPER` | N-DESIGN | 否 |
| `N-VERIFY-V1` | 独立验证第一版实现 | `VERIFIER` | N-IMPLEMENT-V1 | 否 |
| `N-DIAGNOSIS` | 定位第一版 state 过期判断使用错误时钟的根因 | `VERIFIER` | N-VERIFY-V1 | 否 |
| `N-IMPLEMENT-V2` | 依据根因修复 state 过期判断 | `DEVELOPER` | N-DIAGNOSIS | 否 |
| `N-VERIFY-V2` | 使用新环境独立复验修复版本 | `VERIFIER` | N-IMPLEMENT-V2 | 否 |
| `N-INTEGRATE` | 将通过验收的实现合入隔离集成分支 | `DEVELOPER` | N-VERIFY-V1, N-VERIFY-V2 | 否 |
| `N-SYSTEM-VERIFY` | 执行登录端到端与回归验收 | `SENIOR_VERIFIER` | N-INTEGRATE | 否 |
| `N-UAT-PREP` | 启动隔离预览并完成冒烟检查 | `UAT_FACILITATOR` | N-SYSTEM-VERIFY | 否 |
| `N-UAT` | 由用户验收登录错误提示与跳转体验 | `USER` | N-UAT-PREP | 是 |

## 用户验收

### 由用户验收登录错误提示与跳转体验

- 验收舱：`UAC-LOGIN-1` / `WEB_PREVIEW`
- 环境状态：`CLEANED`
- 有效时间：120 分钟
- 本节工作：由用户验收登录错误提示与跳转体验
- 验收产物：`ART-UAT-PREVIEW`
- 产物哈希：`sha256:630fe7832db9471ff3f617d35b6133726bfd32dbaaf9ac794daccaf6660ef5d7`

#### 查看无效链接提示

1. 打开预置的无效登录链接
2. 确认页面说明登录链接已失效
3. 点击重新登录
4. 使用测试账户完成登录

预期结果：提示容易理解，重新登录后回到原目标页面

反馈选项：通过、通过并留下建议、不通过、环境问题，暂未完成测试、不确定，需要演示或更简单说明、委托 Agent 验收

