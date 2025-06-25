import streamlit as st
import random

# 设置页面为宽屏模式
st.set_page_config(layout="wide")

# 应用标题
st.title("Digital Twin")

# ------------------- 组件定义 (Component Class) -------------------
class Component:
    """代表一个机台模块的Python类。"""
    def __init__(self, id, css_class, text="", status_key=None):
        self.id = id
        self.css_class = css_class
        self.text = text
        self.status_key = status_key # 对应 session_state.machine_status 中的键

    def render(self):
        """根据当前状态生成组件的HTML。"""
        status_info = st.session_state.machine_status.get(self.status_key, {})
        status = status_info.get("status", "可用")
        status_class = get_status_class(status)
        
        display_html = ""
        # 根据模块类型构建显示文本
        if "loadport" in self.css_class and status != "可用":
            display_html = f"""
                <div>{self.text}</div>
                <div class="status-text">
                LOT: {status_info.get('lot_id', 'N/A')}<br>
                待处理: {status_info.get('waiting_wafers', 0)}<br>
                已完成: {status_info.get('processed_wafers', 0)}
                </div>
            """
        elif ("p-chamber" in self.css_class or "degas-chamber" in self.css_class) and status != "可用":
            display_html = f"""
                <div>{self.text}</div>
                <div class="status-text">
                Wafer: {status_info.get('wafer_id', 'N/A')}
                </div>
            """
        else:
            if self.text:
                display_html = f'<div>{self.text}</div><div class="status-text">({status})</div>'
            # else: display_html remains "" for components without text

        return f'<div id="{self.id}" class="component {self.css_class} {status_class}">{display_html}</div>'


# ------------------- 自定义 CSS 样式 -------------------
CSS_STYLE = """
<style>
/* --- Main App Styling --- */
[data-testid="stAppViewContainer"] h1 {
    font-size: 2.2rem;
}
[data-testid="stAppViewContainer"] h3 {
    font-size: 1.5rem;
}
div[data-testid="stTextInput"] {
    margin-bottom: -15px; /* Reduce space after input */
}
div[data-testid="stButton"] {
    margin-top: 20px;
}
table {
    font-size: 0.9rem;
    border: none;
}
[data-testid="stTable"] table {
    border-collapse: collapse;
}
[data-testid="stTable"] th, [data-testid="stTable"] td {
    border: none !important;
    padding: 6px 8px;
}
[data-testid="stTable"] thead {
    background-color: transparent !important;
}
[data-testid="stTable"] > div:first-of-type {
    border: none !important;
    box-shadow: none !important;
}
[data-testid="stTable"] td:nth-child(2), [data-testid="stTable"] th:nth-child(2) {
    text-align: center;
}

.machine-layout {
    position: relative;
    width: 700px;
    height: 800px;
    margin: auto;
    transform: scale(0.9);
    transform-origin: top center;
    background-color: white; /* Ensure background for screenshot */
}
.component {
    position: absolute;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    font-weight: bold;
    text-align: center;
    box-shadow: 0 4px 8px 0 rgba(0,0,0,0.2);
    transition: 0.3s;
    color: #333;
    font-size: 13px;
}
.component .status-text {
    font-size: 11px;
    margin-top: 2px;
    line-height: 1.2;
}

/* --- Shapes & Default Colors --- */
.p-chamber { width: 100px; height: 100px; border-radius: 50%; background-color: #FBC696; }
.degas-chamber { width: 100px; height: 100px; border-radius: 50%; background-color: #A3D8B4; }
.loadport { width: 110px; height: 110px; border-radius: 15px; background-color: #F7E59E; }
.robot { width: 150px; height: 150px; border-radius: 50%; background-color: #D6EAF8; }
.buffer { width: 35px; height: 35px; border-radius: 50%; background-color: #EAECEE; }
.loadlock { width: 45px; height: 45px; border-radius: 50%; background-color: #85929E; }
.aligner { width: 70px; height: 90px; border-radius: 10px; background-color: #F7E59E; }
.efem-bar { width: 280px; height: 30px; border-radius: 10px; background-color: #FBC696; }

/* --- Status Colors --- */
.available { border: 4px solid #28a745; }
.occupied { border: 4px solid #dc3545; background-color: #E74C3C !important; color: white;}

/* --- Positioning (top, left) --- */
#chamber-b { top: 10px; left: 215px; }
#chamber-c { top: 10px; left: 385px; }
#vtm-robot { top: 80px; left: 275px; }
#chamber-a { top: 130px; left: 130px; }
#chamber-d { top: 130px; left: 470px; }

#buffer-1 { top: 245px; left: 300px; }
#buffer-2 { top: 245px; left: 365px; }

#degas-a { top: 320px; left: 140px; }
#efem-robot { top: 295px; left: 275px; }
#degas-b { top: 320px; left: 460px; }

#aligner-a { top: 460px; left: 200px; transform: rotate(-15deg); }
#aligner-b { top: 460px; left: 430px; transform: rotate(15deg); }

#loadlock-1 { top: 480px; left: 300px; }
#loadlock-2 { top: 480px; left: 355px; }

#efem-bar { top: 550px; left: 210px; }

#loadport-a { top: 610px; left: 150px; }
#loadport-b { top: 610px; left: 295px; }
#loadport-c { top: 610px; left: 440px; }
</style>
"""
st.markdown(CSS_STYLE, unsafe_allow_html=True)

# ------------------- 状态定义 -------------------

# 初始化或重置状态
def initialize_state():
    st.session_state.machine_status = {
        # 每个组件的状态现在是一个字典
        "Chamber A": {"status": "可用", "wafer_id": None},
        "Chamber B": {"status": "可用", "wafer_id": None},
        "Chamber C": {"status": "可用", "wafer_id": None},
        "Chamber D": {"status": "可用", "wafer_id": None},
        "Degas A": {"status": "可用", "wafer_id": None},
        "Degas B": {"status": "可用", "wafer_id": None},
        "Loadport A": {"status": "可用", "lot_id": None, "waiting_wafers": 0, "processed_wafers": 0},
        "Loadport B": {"status": "可用", "lot_id": None, "waiting_wafers": 0, "processed_wafers": 0},
        "Loadport C": {"status": "可用", "lot_id": None, "waiting_wafers": 0, "processed_wafers": 0},
        "EFEM Robot": {"status": "可用"},
        "VTM Robot": {"status": "可用"}
    }
    # 初始化时也生成虚拟排队信息
    st.session_state.lot_queue = [
        {"LOT ID": f"LOT-{random.randint(201,300)}", "Wafer 数量": 25},
        {"LOT ID": f"LOT-{random.randint(301,400)}", "Wafer 数量": 25},
    ]
    st.session_state.machine_id_input = ""
    st.session_state.timestamp_input = ""


if 'machine_status' not in st.session_state:
    initialize_state()

def get_status_class(status):
    """获取状态对应的CSS类"""
    return "available" if "可用" in status else "occupied"

# ------------------- 布局定义 (Layout Definition) -------------------
MACHINE_LAYOUT = [
    # Process Chambers & VTM
    Component("chamber-b", "p-chamber", "Chamber B", "Chamber B"),
    Component("chamber-c", "p-chamber", "Chamber C", "Chamber C"),
    Component("vtm-robot", "robot", "VTM Robot", "VTM Robot"),
    Component("chamber-a", "p-chamber", "Chamber A", "Chamber A"),
    Component("chamber-d", "p-chamber", "Chamber D", "Chamber D"),
    # Buffers
    Component("buffer-1", "buffer"),
    Component("buffer-2", "buffer"),
    # Degas & EFEM
    Component("degas-a", "degas-chamber", "Degas A", "Degas A"),
    Component("efem-robot", "robot", "EFEM Robot", "EFEM Robot"),
    Component("degas-b", "degas-chamber", "Degas B", "Degas B"),
    # Aligners & Loadlocks
    Component("aligner-a", "aligner"),
    Component("aligner-b", "aligner"),
    Component("loadlock-1", "loadlock"),
    Component("loadlock-2", "loadlock"),
    # EFEM Bar
    Component("efem-bar", "efem-bar"),
    # Loadports
    Component("loadport-a", "loadport", "Loadport A", "Loadport A"),
    Component("loadport-b", "loadport", "Loadport B", "Loadport B"),
    Component("loadport-c", "loadport", "Loadport C", "Loadport C"),
]

# 动态生成机台的HTML
def generate_machine_html():
    components_html = "".join([comp.render() for comp in MACHINE_LAYOUT])
    return f'<div class="machine-layout">{components_html}</div>'

machine_html_content = generate_machine_html()


# ------------------- 回调函数 (Callbacks) -------------------
def traceback_callback():
    """根据输入追溯机台状态（当前为模拟）。"""
    machine_id = st.session_state.machine_id_input
    timestamp = st.session_state.timestamp_input

    if not machine_id or not timestamp:
        st.session_state.info_message = "错误：请输入机台ID和时间点。"
        return

    # --- 模拟生成详细的随机状态 ---
    st.session_state.info_message = f"成功追溯机台 {machine_id} 在 {timestamp} 的状态。"
    
    # 模拟Chamber状态
    for chamber in ["Chamber A", "Chamber B", "Chamber C", "Chamber D", "Degas A", "Degas B"]:
        if random.choice([True, False]):
            st.session_state.machine_status[chamber] = {
                "status": "处理中",
                "wafer_id": f"LOT-{random.randint(100,200)}-W{random.randint(1,25)}"
            }
        else:
            st.session_state.machine_status[chamber] = {"status": "可用", "wafer_id": None}

    # 模拟Loadport状态
    for port in ["Loadport A", "Loadport B", "Loadport C"]:
        if random.choice([True, False]):
            st.session_state.machine_status[port] = {
                "status": "占用中",
                "lot_id": f"LOT-{random.randint(1,100)}",
                "waiting_wafers": random.randint(0, 25),
                "processed_wafers": random.randint(0, 25)
            }
        else:
            st.session_state.machine_status[port] = {"status": "可用", "lot_id": None, "waiting_wafers": 0, "processed_wafers": 0}

    # 模拟机器人状态
    st.session_state.machine_status["EFEM Robot"]["status"] = random.choice(["可用", "工作中"])
    st.session_state.machine_status["VTM Robot"]["status"] = random.choice(["可用", "工作中"])
    
    # 模拟排队信息
    st.session_state.lot_queue = [
        {"LOT ID": f"LOT-{random.randint(201,300)}", "Wafer 数量": 25},
        {"LOT ID": f"LOT-{random.randint(301,400)}", "Wafer 数量": 25},
    ]

# ------------------- Page Layout -------------------

# Create main columns with a spacer for better visual separation
left_col, middle_col, spacer, right_col = st.columns([2.8, 1, 0.2, 1.2])

with left_col:
    st.markdown(machine_html_content, unsafe_allow_html=True)

with middle_col:
    st.subheader("机台排队信息")
    if st.session_state.lot_queue:
        st.table(st.session_state.lot_queue)
    else:
        st.write("无排队信息。")

with spacer:
    st.empty() # This column is just for spacing

with right_col:
    st.subheader("状态追溯查询")
    st.text_input("机台 (Machine ID)", key="machine_id_input")
    st.caption("例如: ALD-01")
    st.text_input("时间点 (Timestamp)", key="timestamp_input")
    st.caption("例如: 2023-10-27 10:30:00")
    st.button("Traceback", on_click=traceback_callback, use_container_width=True)
    
    # 显示信息或错误
    if st.session_state.get("info_message"):
        st.info(st.session_state.info_message)
    




