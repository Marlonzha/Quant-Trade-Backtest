import json
import pandas as pd
from tqsdk import TqApi, TqAuth, TqBacktest, TargetPosTask, TqSim, TqMultiAccount
from datetime import date
from tqsdk.tafunc import ma, ema
from tqsdk.exceptions import BacktestFinished
import math
import threading
import time

def calculate_vwma(data, window_size):
    """
    计算成交量加权移动平均线（VWMA）
    :param data: 包含日期、收盘价和成交量的数据框
    :param window_size: 计算VWMA的窗口大小
    :return: 包含原始数据和VWMA的数据框
    """
    data = data.copy()
    data['close'] = pd.to_numeric(data['close'])
    data['volume'] = pd.to_numeric(data['volume'])
    data['价格_成交量乘积'] = data['close'] * data['volume']
    data['VWMA'] = data['价格_成交量乘积'].rolling(window=window_size).sum() / data['volume'].rolling(window=window_size).sum()
    return data["VWMA"]

def create_run_strategy(klines, strategy_params, target_pos_i):
    strategy_type = strategy_params["X_MA"]
    strategy_short = strategy_params["short"]
    strategy_long = strategy_params["long"]
    strategy_target_pos = target_pos_i
    if strategy_type == "MA":
        short_avg = ma(klines["close"], strategy_short)
        long_avg = ma(klines["close"], strategy_long)
    elif strategy_type == "EMA":
        short_avg = ema(klines["close"], strategy_short)
        long_avg = ema(klines["close"], strategy_long)
    elif strategy_type == "VWMA":
        short_avg = calculate_vwma(klines, strategy_short)
        long_avg = calculate_vwma(klines, strategy_long)
    elif strategy_type == "MA+kσ":
        short_avg = ma(klines["close"], strategy_short)
        long_avg_top = ma(klines["close"], strategy_long) + klines["close"].rolling(window=strategy_long).std() * 0.2
        long_avg_bottom = ma(klines["close"], strategy_long) - klines["close"].rolling(window=strategy_long).std() * 0.2
        if long_avg_bottom.iloc[-3] < short_avg.iloc[-3] and long_avg_bottom.iloc[-2] > short_avg.iloc[-2]:
            strategy_target_pos.set_target_volume(-10)
        if short_avg.iloc[-3] < long_avg_top.iloc[-3] and short_avg.iloc[-2] > long_avg_top.iloc[-2]:
            strategy_target_pos.set_target_volume(10)
        return
    else:
        print("X_MA参数输入错误")
        return

    if long_avg.iloc[-3] > short_avg.iloc[-3] and long_avg.iloc[-2] < short_avg.iloc[-2]:
        strategy_target_pos.set_target_volume(10)
    if short_avg.iloc[-3] > long_avg.iloc[-3] and short_avg.iloc[-2] < long_avg.iloc[-2]:
        strategy_target_pos.set_target_volume(-10)
    return

def create_backtest_record(R):
    data = {
        "策略-参数1": R["param_1"],
        "策略-参数2": R["param_2"],
        "策略-参数3": R["param_3"],
        "标的": R["标的"],
        "开始时间": R["start_date"],
        "结束时间": R["end_date"],
        "保证金": R["保证金"],
        "使用金额": max(R["保证金"] * 10 * 2, R["report"]["max_drawdown"] * R["report"]["start_balance"] * 2),
        "起始金额": R["report"]["start_balance"],
        "结束金额": R["report"]["end_balance"],
        "最大回撤": R["report"]["max_drawdown"] * R["report"]["start_balance"] / (R["保证金"] * 10 * 2) * 100,
        "盈亏比": R["report"]["profit_loss_ratio"],
        "胜率": R["report"]["winning_rate"],
        "收益率": (R["report"]["end_balance"] - R["report"]["start_balance"]) / max(R["保证金"] * 10 * 2,
        R["report"]["max_drawdown"] * R["report"]["start_balance"] * 2) * 100
    }
    return pd.DataFrame(data, index=[0])

def create_params(symbol, strategy_name_list):
    symbol, start_date, end_date = symbol["symbol"], symbol["start_date"], symbol["end_date"]
    y, m, d = map(int, start_date.split("-"))
    e_y, e_m, e_d = map(int, end_date.split("-"))
    start_date = date(y, m, d)
    end_date = date(e_y, e_m, e_d)
    long = max([int(strategy_name_list[i]["long"]) for i in range(len(strategy_name_list))])
    return symbol, start_date, end_date, long

def run_backtest(symbol, strategy_name_list, user, result_dict_list, completed_symbols):
    symbol, start_date, end_date, long = create_params(symbol, strategy_name_list)
    if symbol in completed_symbols:
        print("{}的回测已完成，跳过".format(symbol))
        return
    acc_list = [TqSim(10000000) for _ in range(len(strategy_name_list))]
    api = TqApi(account=TqMultiAccount(acc_list), backtest=TqBacktest(start_dt=start_date, end_dt=end_date), auth=TqAuth(user["username"], user["password"]))
    quate = api.get_quote(symbol)
    margin = quate.margin
    klines = api.get_kline_serial(symbol, duration_seconds=60*60*24, data_length=long+3)
    target_pos_list = [TargetPosTask(api, symbol, account=acc_list[i]) for i in range(len(acc_list))]
    print("开始运行{}的回测".format(symbol))
    try:
        while True:
            api.wait_update()
            if api.is_changing(klines.iloc[-1], "datetime"):
                for i in range(len(strategy_name_list)):
                    create_run_strategy(klines, strategy_name_list[i], target_pos_list[i])
    except BacktestFinished:
        print("回测结束")
        api.close()
        for r in range(len(acc_list)):
            result_dict = {
                "param_1": strategy_name_list[r]["X_MA"],
                "param_2": str(strategy_name_list[r]["short"]),
                "param_3": str(strategy_name_list[r]["long"]),
                "标的": symbol,
                "保证金": margin,
                "start_date": start_date,
                "end_date": end_date,
                "report": acc_list[r].tqsdk_stat
            }
            result_dict_list.append(create_backtest_record(result_dict))
        completed_symbols.add(symbol)  # 记录已完成的回测标的

class MyThread(threading.Thread):
    def __init__(self, thread_id, symbol, strategy_name_list, user, result_dict_list, completed_symbols):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.symbol = symbol
        self.strategy_name_list = strategy_name_list
        self.user = user
        self.result_dict_list = result_dict_list
        self.completed_symbols = completed_symbols

    def run(self):
        print("开始线程：{}".format(self.thread_id))
        run_backtest(self.symbol, self.strategy_name_list, self.user, self.result_dict_list, self.completed_symbols)
        print("退出线程：{}".format(self.thread_id))


# 第一部分：配置文件目录
symbols_pool_path = r"C:/Users/zhaoc/OneDrive/LH2024/HL2024-flask/策略研究/双均线策略/Symbols_and_Strategy/20250319标的池.json"
strategy_pool_path = r"C:/Users/zhaoc/OneDrive/LH2024/HL2024-flask/策略研究/双均线策略/Symbols_and_Strategy/策略池.json"
user_path = r"C:/Users/zhaoc/OneDrive/LH2024/HL2024-flask/策略研究/双均线策略/Symbols_and_Strategy/账户.json"
completed_symbols_path = r"C:/Users/zhaoc/OneDrive/LH2024/HL2024-flask/策略研究/双均线策略/已完成标的.json"
backtest_record_path = r"C:/Users/zhaoc/OneDrive/LH2024/HL2024-flask/策略研究/双均线策略/策略回测结果_{}.csv".format(date.today().strftime("%Y%m%d"))

# 第二部分:读取配置信息
with open(user_path, 'r', encoding='utf8') as fp:
    user = json.load(fp)['TQ_acc']

with open(symbols_pool_path, 'r', encoding='utf8') as fp:
    symbols = list(json.load(fp)['回测池——20250320'].values())

with open(strategy_pool_path, 'r', encoding='utf8') as fp:
    strategy_list = json.load(fp)['双均线策略-均线类型']
    param_list = json.load(fp)['双均线策略-均线周期']
    strategy_name_list = [{"X_MA": strategy, "short": param[0], "long": param[1]} for strategy in strategy_list.values() for param in param_list.values()]

# 读取已完成的标的
try:
    with open(completed_symbols_path, 'r', encoding='utf8') as fp:
        completed_symbols = set(json.load(fp))
except FileNotFoundError:
    completed_symbols = set()

# 第三部分多线程运行回测
start_backtest_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
print("开始回测时间:{}".format(start_backtest_time))

result_dict_list = []
thread_list = []
for i, symbol in enumerate(symbols):
    thread = MyThread(i, symbol, strategy_name_list, user, result_dict_list, completed_symbols)
    thread.start()
    thread_list.append(thread)

try:
    for t in thread_list:
        t.join()
except KeyboardInterrupt:
    print("手动中断程序")

print("退出主线程")

# 添加回测结果到文件下
backtest_record_df = pd.concat(result_dict_list)
backtest_record_df.to_csv(backtest_record_path, index=False)

# 保存已完成的标的
with open(completed_symbols_path, 'w', encoding='utf8') as fp:
    json.dump(list(completed_symbols), fp)

end_backtest_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
print("开始回测时间:{}".format(start_backtest_time))
print("结束回测时间:{}".format(end_backtest_time))
