# 🤖 极致动量风控军师系统 (V2.0)

> **与 `ibkr-order-tool` 完全独立。** 侧边栏要出现 `RiskManager_System` 文件夹，需先在本机 **clone 独立 GitHub 仓库**（见下方「Cursor 侧边栏」）。

## 📁 目录结构
你的项目文件夹 `RiskManager_System` 应该包含以下文件：
- `requirements.txt` (环境依赖)
- `database_setup.py` (数据库初始化脚本)
- `main.py` (核心风控引擎)
- `README.md` (本说明文件)
- `run.bat` (Windows 一键启动，可选)

## 🚀 PC 端首次启动步骤 (保姆级指南)

### 第一步：配置密钥 (极其重要)
用电脑端的代码编辑器（如 Cursor/VSCode）打开 `main.py`。
在代码的第 16 行至第 24 行，填入你的真实参数：
1. `MY_TELEGRAM_CHAT_ID`: 填入 `@userinfobot` 给你的数字 ID。
2. `TG_BOT_TOKEN`: 填入 `@BotFather` 给你的长串 Token。
3. `TWS_PORT`: 模拟盘保持 `7497`，实盘改为 `7496` (IB Gateway 对应 4002/4001)。
4. `FLEX_TOKEN` & `FLEX_QUERY_ID`: 填入盈透后台生成的查询密钥和 ID。
5. `NOTION_TOKEN` & `NOTION_DATABASE_ID`: 填入 Notion 授权码和数据库 ID。

### 第二步：安装环境与建库
在项目文件夹的地址栏输入 `cmd` 打开终端，依次运行：
```bash
# 1. 安装所有需要的第三方库
pip install -r requirements.txt

# 2. 生成本地 SQLite 数据库表
python database_setup.py
```
> **成功标志：** 终端提示“✅ 极致风控数据库 risk_manager.db 初始化成功！”，且文件夹内多出一个 .db 文件。

> **提示：** 若跳过第 2 步直接运行 `main.py`，程序也会自动建表；但建议首次仍执行 `database_setup.py` 确认环境正常。

### 第三步：连接 TWS 与启动系统
1. 打开 TWS 桌面软件，登录**二级用户**或**模拟盘账号**。
2. 确保 TWS 设置中的 API 已勾选“启用 ActiveX 和 Socket 客户端”。
3. 在终端运行主程序：
```bash
python main.py
```
> **成功标志：** 终端打印出“✅ TWS 已连接，成交监听已启动。”与“✅ Telegram Bot 已启动。”，Telegram 可发送 `/start` 测试。

**Windows 用户：** 也可双击 `run.bat` 启动（需已安装 Python 且 `python` 在 PATH 中）。

## 📱 手机端盘中操作指令速查
- `/init [代码] [止损价] [策略标签]` → 录入新仓位，接受大师级 Checklist 审查。
- `/status` → 查看净值、风控灯与 OPEN 持仓。
- `/update [代码] [新止损价]` → 盘中上移止损，释放风险额度。
- `/split [代码] [拆/合股比例]` → 极简处理拆股合股（如1拆4输入4，4合1输入0.25）。
- `/rename [旧代码] [新代码]` → 处理上市公司更名。
- `/override [代码] [坦白理由]` → 深夜 23:00 强制坦白协议，记录冲动违规交易。
- `/sync` → 手动触发 Flex 盘前对账。

## 📦 文件传输到电脑
将 `RiskManager_System` 整个文件夹通过微信文件传输助手、坚果云或 iCloud 拷到 PC，按上述三步部署即可。

## 🖥️ 在 Cursor 侧边栏单独出现（必读）

侧边栏里的 **「风控军师系统」是 Agent 对话记录**，挂在 `ibkr-order-tool` 下面，**不是**独立项目文件夹。

要让侧边栏出现与 `ibkr-order-tool` 平级的 `RiskManager_System`，按下面做：

### ① 在 GitHub 新建空仓库（约 1 分钟）
1. 打开 https://github.com/new
2. Repository name 填：`RiskManager_System`
3. 选 Public，**不要**勾选 Add README
4. 点 Create repository

### ② 在本机 clone（与 ibkr-order-tool 同级目录）
```bash
cd ~/你的项目父目录    # 和 ibkr-order-tool 同一层
git clone https://github.com/gengxianglong-star/RiskManager_System.git
```

### ③ 在 Cursor 打开
**File → Open Folder → 选刚 clone 的 `RiskManager_System`**

成功后侧边栏会出现独立项目，与 `a-share-market-monitor`、`ibkr-order-tool` 平级。

> 仓库建好后，在 Agent 对话里回复「仓库已建好」，可把完整源码 push 上去。

## 🖥️ 在 Cursor 中单独开项目（与交易工具分开）
本项目**不要**放在 `ibkr-order-tool` 目录下，应作为独立工作区打开：

1. Cursor 菜单 **File → Open Folder…**
2. 选择 `RiskManager_System` 文件夹（不要选外层 `ibkr-order-tool`）
3. 确认左侧资源管理器根目录名是 `RiskManager_System`，且只有本项目的 6 个文件

两个项目应分别打开、分别维护：
| 项目 | 用途 |
|------|------|
| `ibkr-order-tool` | 桌面 PyQt 下单工具 |
| `RiskManager_System` | Telegram 风控军师 |
