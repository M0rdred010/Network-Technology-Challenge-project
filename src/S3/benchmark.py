import subprocess
import time
import sys
import matplotlib.pyplot as plt

def run_and_time(script_name, folder_name, no_save=False):
    print(f"[{script_name}] 开始运行 (数据目录: {folder_name})...")
    start_time = time.time()
    
    try:
        # 使用 sys.executable 确保使用当前同一个 Python 解释器
        cmd = [sys.executable, script_name, folder_name]
        if no_save:
            cmd.append('--no-save')
        subprocess.run(cmd, check=True)
        end_time = time.time()
        duration = end_time - start_time
        print(f"[{script_name}] [{folder_name}] 运行完成！耗时: {duration:.2f} 秒\n")
        return duration
    except subprocess.CalledProcessError as e:
        print(f"\n[错误] 运行 {script_name} 失败，退出码: {e.returncode}")
        return None
    except KeyboardInterrupt:
        print(f"\n[非正常退出] 用户手动中断了 {script_name} 的运行。")
        return None

def main():
    print("="*60)
    print("        自动化性能比对测试 (多规模拓扑)        ")
    print("="*60)
    
    # 定义测试的文件夹以及对应的卫星数量
    test_cases = [
        {"folder": "sat_trace", "sat_count": 25},
        {"folder": "sat_trace_50", "sat_count": 50},
        {"folder": "sat_trace_100", "sat_count": 100},
        {"folder": "sat_trace_150", "sat_count": 150},
        {"folder": "sat_trace_200", "sat_count": 200},
    ]

    counts = []
    times_original = []
    times_optimized = []

    for case in test_cases:
        folder = case["folder"]
        sat_count = case["sat_count"]
        print(f"\n======== 测试规模: {sat_count} 卫星 ========")
        
        # 对于节点数目大于 25 的，不输出文件以免占用过多硬盘空间
        skip_save = (sat_count > 25)
        
        # 跑原版
        t_orig = run_and_time("s3.py", folder, skip_save)
        # 跑优化版
        t_opt = run_and_time("s3_optimized.py", folder, skip_save)
        
        counts.append(sat_count)
        times_original.append(t_orig if t_orig else 0)
        times_optimized.append(t_opt if t_opt else 0)

    # 打印终端汇总表格
    print("\n" + "="*60)
    print("                      测试结果汇总                      ")
    print("="*60)
    print(f"{'卫星数量':<8} | {'原版耗时 (s)':<14} | {'优化版耗时 (s)':<14} | {'提升倍数':<0}")
    print("-" * 60)
    for c, to, tp in zip(counts, times_original, times_optimized):
        speedup = (to / tp) if tp > 0 else 0
        print(f"{c:<12} | {to:<18.2f} | {tp:<18.2f} | {speedup:.2f}x")
    print("="*60)

    # =================
    # 使用 matplotlib 画图
    # =================
    plt.figure(figsize=(10, 6))
    
    # 画线
    plt.plot(counts, times_original, marker='o', linestyle='-', color='tab:red', linewidth=2, label='Original (s3.py)')
    plt.plot(counts, times_optimized, marker='s', linestyle='-', color='tab:green', linewidth=2, label='Optimized (s3_optimized.py)')
    
    # 标注数值标签
    for i, txt in enumerate(times_original):
        if txt > 0:
            plt.annotate(f"{txt:.1f}s", (counts[i], times_original[i]), textcoords="offset points", xytext=(0,10), ha='center', color='tab:red')
            
    for i, txt in enumerate(times_optimized):
        if txt > 0:
            plt.annotate(f"{txt:.1f}s", (counts[i], times_optimized[i]), textcoords="offset points", xytext=(0,-15), ha='center', color='tab:green')
    
    # 图表装饰
    plt.title('Algorithm Performance Comparison: Original vs Optimized', fontsize=14, pad=15)
    plt.xlabel('Number of Satellites (Network Scale)', fontsize=12)
    plt.ylabel('Execution Time (Seconds)', fontsize=12)
    plt.xticks(counts)  # 让X轴精确对应我们测试的几个数值点
    
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.legend(fontsize=11)
    
    # 保存并展示
    plot_filename = "performance_comparison_scaling.png"
    plt.savefig(plot_filename, dpi=300, bbox_inches='tight')
    print(f"\n✅ 对比折线图表已成功保存为本目录下的: {plot_filename}")
    
    try:
        # 打开窗口展示
        plt.show()
    except Exception as e:
        print("（终端无GUI环境，已跳过弹出窗口展示）")

if __name__ == "__main__":
    main()
