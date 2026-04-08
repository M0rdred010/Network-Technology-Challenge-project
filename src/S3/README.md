# S3 Topology Project (Reorganized)

本项目已按功能重新整理目录，并将默认入口切换为优化版本。

## 目录结构

```text
s3/
├─ s3.py                          # 主入口（已替换为 optimized）
├─ README.md
├─ algorithms/                    # 六算法与统一仿真核心
│  ├─ s3_routing_core.py
│  ├─ s3_optimized.py
│  ├─ s3_hypatia.py
│  ├─ s3_lsr.py
│  ├─ s3_madrl.py
│  ├─ s3_ftrl.py
│  ├─ s3_dtn_cgr.py
│  └─ benchmark_algorithms.py     # 六算法横评脚本
├─ traces/                        # 全部轨迹数据
│  ├─ sat_trace/
│  ├─ sat_trace_50/
│  ├─ sat_trace_100/
│  ├─ sat_trace_150/
│  ├─ sat_trace_200/
│  ├─ uav_trace/
│  └─ uav_trace_full.csv
├─ outputs/                       # 所有输出文件
│  ├─ output*/
│  ├─ output_sat_trace_*/
│  └─ benchmark_results/
├─ docs/                          # 文档与历史脚本
│  ├─ Optimization_Readme.md
│  └─ benchmark_legacy.py
└─ test/                          # 复现与实验参考代码
```

## 默认行为变更

- 根目录 `s3.py` 已改为优化算法入口，相当于运行 `optimized`。
- 算法脚本默认从 `traces/` 读取数据。
- 新输出统一写入 `outputs/`。

## 快速开始

### 1) 跑默认优化版

```bash
python s3.py
```

### 2) 跑某个算法（示例：Hypatia）

```bash
python algorithms/s3_hypatia.py
```

### 3) 六算法横评

```bash
python algorithms/benchmark_algorithms.py
```

可选参数示例：

```bash
python algorithms/benchmark_algorithms.py traces/sat_trace_100 --max-steps 300 --save-outputs
```

## 主要输出位置

- 单算法输出：`outputs/output_<sat_count>/`（例如 `output_25`、`output_50`）
- 横评汇总：`outputs/benchmark_results/`
  - `algorithm_benchmark.csv`
  - `algorithm_benchmark.json`
  - `algorithm_benchmark.png`

## 备注

- 若使用 Windows 且命令 `python` 指向不一致，请改用你本机解释器完整路径。
- `test/` 目录保留为对照与复现材料，不参与当前主流程。
