import pandas as pd
import numpy as np
from scipy.spatial import cKDTree
import json
import math
import networkx as nx
import glob
import random
import os
from collections import defaultdict
import hashlib

# --- 全局配置 ---
MAX_LINK_RANGE = 5000 * 1000  
MIN_ELEVATION_DEG = 10.0      
SPEED_OF_LIGHT = 3e8
TOPO_HASH_INTERVAL = 100  # 每 100 步检查一次拓扑变化
from s3_routing_core import run_cli


if __name__ == "__main__":
    run_cli("optimized")
