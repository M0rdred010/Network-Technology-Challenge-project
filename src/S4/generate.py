import random

def generate_sar_traffic(uav_list, main_gs, max_time_ms=600000): 
    requests = []
    
    for current_time in range(0, max_time_ms, 100):
        
        if current_time % 1000 == 0:
            for uav in uav_list:
                requests.append({
                    'time': current_time + random.randint(0, 50),
                    'node_id': main_gs,
                    'content_id': f'telemetry_{uav}'
                })
        
        # 阶段 1: 常规搜索 (0-30s)，地面站拉取低清图像
        if current_time < 30000 and current_time % 2000 == 0:
            for uav in uav_list:
                requests.append({
                    'time': current_time + random.randint(0, 100),
                    'node_id': main_gs,      # 请求方是地面站
                    'content_id': f'low_res_img_{uav}'
                })
        
        # 阶段 2: 发现目标 (30s 之后)！地面站疯狂拉取高清视频
        if current_time >= 30000 and current_time % 500 == 0:
            requests.append({
                'time': current_time,
                'node_id': main_gs,          # 请求方是地面站
                'content_id': '4k_video_stream'
            })
        
        if current_time == 35000:
            for uav in uav_list:
                if uav != 'UAV_02': # 假设 UAV_02 忙着发视频，其他飞机去支援
                    requests.append({
                        'time': current_time,
                        'node_id': uav,       # 请求方是无人机
                        'content_id': 'c2_converge_cmd'
                    })
        
    # 按 'time' 排序，适配主循环判断
    requests.sort(key=lambda x: x['time'])
    return requests


def generate_uav_requests(uav_list, max_time_ms=600000):
    """
    生成无人机之间的通信请求
    
    Args:
        uav_list: 无人机列表
        max_time_ms: 最大模拟时间（毫秒）
        
    Returns:
        按时间排序的请求列表
    """
    requests = []
    
    for current_time in range(0, max_time_ms, 100):
        
        # 无人机之间的状态共享（每1500ms）
        if current_time % 1500 == 0:
            for i, uav in enumerate(uav_list):
                # 随机选择一个其他无人机作为目标
                target_uav = random.choice([u for u in uav_list if u != uav])
                requests.append({
                    'time': current_time + random.randint(0, 50),
                    'node_id': uav,       # 请求方是无人机
                    'content_id': f'status_update_{uav}'
                })
        
        # 目标位置信息共享（每3000ms）
        if current_time % 3000 == 0 and current_time >= 20000:  # 20秒后开始
            for uav in uav_list:
                requests.append({
                    'time': current_time + random.randint(0, 100),
                    'node_id': uav,       # 请求方是无人机
                    'content_id': 'target_location_update'
                })
        
        # 协作任务请求（每4500ms）
        if current_time % 4500 == 0 and current_time >= 25000:  # 25秒后开始
            # 随机选择一个无人机作为任务发起方
            task_initiator = random.choice(uav_list)
            # 随机选择一个无人机作为协作对象
            task_partner = random.choice([u for u in uav_list if u != task_initiator])
            requests.append({
                'time': current_time,
                'node_id': task_initiator,       # 请求方是无人机
                'content_id': f'collaboration_request_{task_partner}'
            })
        
        # 紧急情况求助（随机发生）
        if random.random() < 0.005:  # 5%的概率触发紧急求助
            # 随机选择一个无人机作为求助方
            emergency_uav = random.choice(uav_list)
            requests.append({
                'time': current_time,
                'node_id': emergency_uav,       # 请求方是无人机
                'content_id': 'emergency_assistance'
            })
        
        # 燃油状态共享（每5000ms）
        if current_time % 5000 == 0:
            for uav in uav_list:
                requests.append({
                    'time': current_time + random.randint(0, 50),
                    'node_id': uav,       # 请求方是无人机
                    'content_id': f'fuel_status_{uav}'
                })
    
    # 按 'time' 排序，适配主循环判断
    requests.sort(key=lambda x: x['time'])
    return requests