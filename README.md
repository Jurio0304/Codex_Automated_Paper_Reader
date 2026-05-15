# Codex Automated Paper Reader (CAPR)

> 中文 | [English](README.en.md)

CAPR 是一个用 Codex 自动化完成每日论文候选检索、语义筛选和学术日报生成的轻量级工作流。

它把任务拆成两个层次：

1. `paper-daily` 脚本只负责从 arXiv / OpenReview 抓取候选论文、统一元数据、去重和粗排序。
2. Codex 作为科研助理读取候选池，逐篇语义评分，选择高价值论文，并撰写最终 Markdown 日报。

核心原则：脚本只提供候选池和可解释的检索线索，最终推荐与总结由 Codex 完成。

## 功能特性

- 从 arXiv 和 OpenReview 抓取近期论文候选。
- 将不同来源论文统一为一致的 JSON schema。
- 基于 source ID 和标准化标题去重。
- 用透明的规则分数提供候选粗排线索。
- 在抓取前做网络预检，避免网络/代理错误被误写为空候选池。
- 默认绕过某些自动化环境中的失效代理变量。
- 当 arXiv export API 限流时，自动 fallback 到 arXiv HTML recent-list 页面。
- 让 Codex 完成最终阅读、语义评分、排序和日报写作，避免关键词模板式推荐。

## 项目结构

```text
Codex_Automated_Paper_Reader/
├── README.md
├── README.en.md
├── Paper_Reader.template.txt       # 中文自动化 prompt 模板
├── Paper_Reader.template.en.txt    # English automation prompt template
├── LICENSE
└── paper-daily/
    ├── config.yaml
    ├── requirements.txt
    ├── scripts/
    │   ├── daily_papers.py
    │   ├── fetch_arxiv.py
    │   ├── fetch_openreview.py
    │   ├── rank_papers.py
    │   └── utils.py
    └── tests/
```

以下运行产物默认不会提交到 Git：

```text
paper-daily/data/raw/
paper-daily/data/processed/
paper-daily/logs/
paper-daily/reports/
```

## 快速开始

### 1. 克隆项目

```bash
git clone https://github.com/<your-user>/Codex_Automated_Paper_Reader.git
cd Codex_Automated_Paper_Reader/paper-daily
```

### 2. 创建 Python 环境

推荐 Python 3.10+。

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Windows PowerShell 示例：

```powershell
python -m venv .venv-win
.\.venv-win\Scripts\python.exe -m pip install -r requirements.txt
```

### 3. 抓取候选论文

```bash
python scripts/daily_papers.py --config config.yaml --date today --stage fetch --force
```

输出文件：

```text
data/raw/YYYY-MM-DD.json
data/processed/YYYY-MM-DD_candidates.json
logs/YYYY-MM-DD.log
```

注意：这一步只生成候选池，不生成最终日报。

## Codex 自动化用法

CAPR 推荐配合 Codex standalone automation 使用。

1. 复制公开模板。中文模板是默认版本，英文模板可按需使用：

```bash
# 中文模板
cp ../Paper_Reader.template.txt ../Paper_Reader.txt

# English template
cp ../Paper_Reader.template.en.txt ../Paper_Reader.txt
```

2. 编辑本地 `Paper_Reader.txt`：

- 设置运行环境中的 `paper-daily` 工作目录。
- 设置运行环境中的 Python 可执行文件。
- **填写你的研究背景、关注方向和评分偏好。**

3. 在 Codex 中创建定时自动化任务，例如每天早上 8:00 运行一次。

`Paper_Reader.txt` 默认被 `.gitignore` 忽略，因为它通常包含机器相关路径和私有筛选偏好。公开仓库只保留中英文模板文件。


## 候选文件格式

`data/processed/YYYY-MM-DD_candidates.json` 中每篇论文大致包含：

```json
{
  "id": "2605.12345",
  "source": "arxiv",
  "title": "Paper title",
  "authors": ["Author One", "Author Two"],
  "abstract": "Paper abstract...",
  "url": "https://arxiv.org/abs/2605.12345",
  "pdf_url": "https://arxiv.org/pdf/2605.12345",
  "published_at": "2026-05-15T00:00:00+00:00",
  "updated_at": "2026-05-15T00:00:00+00:00",
  "venue": "cs.LG",
  "categories": ["cs.LG", "cs.AI"],
  "keyword_score": 7.0,
  "retrieval_reason": "matched method-transfer terms...",
  "matched_keywords": ["uncertainty estimation"],
  "negative_matches": [],
  "coarse_retrieval_score": 8.0
}
```

`keyword_score` 和 `coarse_retrieval_score` 只是粗筛线索，不是最终推荐依据。

## 推荐的 Codex 阅读流程

抓取完成后，Codex 应该：

1. 读取 `data/processed/YYYY-MM-DD_candidates.json`。
2. 围绕方法相关性、启发价值、可迁移性、论文质量、新颖性和可操作性逐篇评分。
3. 保存评分到 `data/processed/YYYY-MM-DD_scored.json`。
4. 对高分候选进一步打开网页或 PDF 阅读。
5. 写入最终报告 `reports/YYYY-MM-DD.md`。
6. 明确说明每篇总结是基于 abstract-only、paper page 还是 PDF 阅读。

## 配置

编辑 `paper-daily/config.yaml` 可调整：

- 研究画像
- 正向/负向关键词
- arXiv 分类
- OpenReview venue
- 候选数量上限
- arXiv retry 和 HTML fallback 行为
- 输出目录

### 代理说明

有些自动化环境会注入不可用的代理变量。

CAPR 默认会在论文源请求中忽略环境代理，避免误判网络失败。如果你确实需要使用系统代理，可以设置：

```bash
PAPER_DAILY_USE_ENV_PROXY=1
```

## 测试

在 `paper-daily` 目录运行：

```bash
python -m pytest tests
```


## 项目状态

CAPR 目前是一个早期科研自动化工具。当前重点是提升候选检索可靠性、保持推荐过程透明，并让最终文献日报由 Codex 阅读候选后生成，而不是由固定关键词模板生成。

## License

本项目采用 MIT License，详见 [LICENSE](LICENSE)。
