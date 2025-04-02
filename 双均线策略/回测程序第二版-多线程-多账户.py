
import json
import pandas as pd
from tqsdk import TqApi, TqAuth, TqBacktest, TargetPosTask, TqSim, TqMultiAccount
from datetime import date
from tqsdk.tafunc import ma,ema
from tqsdk.exceptions import BacktestFinished
import math
import threading
import time

# strategy_name_list[0]= {'X_MA': 'MA', 'short': 5, 'long': 15}
def calculate_vwma(data, window_size):
    """
    计算成交量加权移动平均线（VWMA）
    :param data: 包含日期、收盘价和成交量的数据框
    :param window_size: 计算VWMA的窗口大小
    :return: 包含原始数据和VWMA的数据框
    """
    # 确保数据框中的价格和成交量列是数值类型
    data = data.copy()
    data['close'] = pd.to_numeric(data['close'])
    data['volume'] = pd.to_numeric(data['volume'])
    # 计算价格与成交量的乘积
    data['价格_成交量乘积'] = data['close'] * data['volume']
    # 计算VWMA
    data['VWMA'] = data['价格_成交量乘积'].rolling(window=window_size).sum() / data['volume'].rolling(window=window_size).sum()
    return data["VWMA"]

def craet_run_strategy(klines,strategy_params,target_pos_i):
    strategy_type = strategy_params["X_MA"]
    strategy_short = strategy_params["short"]
    strategy_long = strategy_params["long"]
    strategy_target_pos = target_pos_i
    if strategy_type == "MA":
        short_avg = ma(klines["close"], strategy_short)  # 短周期
        long_avg = ma(klines["close"],  strategy_long)  # 长周期
    elif strategy_type == "EMA":
        short_avg = ema(klines["close"], strategy_short)  # 短周期
        long_avg = ema(klines["close"],  strategy_long)  # 长周期
    elif strategy_type == "VWMA":
        short_avg = calculate_vwma(klines, strategy_short)  # 短周期
        long_avg = calculate_vwma(klines,  strategy_long)  # 长周期
    elif strategy_type == "MA+kσ":
        short_avg = ma(klines["close"], strategy_short)  # 短周期
        long_avg_top= ma(klines["close"],  strategy_long) + klines["close"].rolling(window=strategy_long).std()*0.2  # 长周期上轨道
        long_avg_bottom = ma(klines["close"],  strategy_long,) -  klines["close"].rolling(window=strategy_long).std()*0.2 # 长周期下轨道
        # 均线下穿，做空
        if long_avg_bottom.iloc[-3] < short_avg.iloc[-3] and long_avg_bottom.iloc[-2] > short_avg.iloc[-2]:
            strategy_target_pos.set_target_volume(-10)
        # 均线上穿，做多
        if short_avg.iloc[-3] < long_avg_top.iloc[-3] and short_avg.iloc[-2] > long_avg_top.iloc[-2]:
            strategy_target_pos.set_target_volume(10) 
        return
    else:
        print("X_MA参数输入错误")
        return 

    # 上穿，做多
    if long_avg.iloc[-3] > short_avg.iloc[-3] and long_avg.iloc[-2] < short_avg.iloc[-2]:
        strategy_target_pos.set_target_volume(10)
    # 下穿，做空
    if short_avg.iloc[-3] > long_avg.iloc[-3] and short_avg.iloc[-2] < long_avg.iloc[-2]:
       strategy_target_pos.set_target_volume(-10)
    return 
def create_backtest_record(R):
    data = {
        "策略-参数1":R["param_1"],
        "策略-参数2":R["param_2"],
        "策略-参数3":R["param_3"],
        "标的":R["标的"],
        "开始时间":R["start_date"],
        "结束时间":R["end_date"],
        "保证金" : R["保证金"],
        "使用金额" : max(R["保证金"]*10*2,R["report"]["max_drawdown"]*R["report"]["start_balance"]*2),
        "起始金额" : R["report"]["start_balance"],
        "结束金额" : R["report"]["end_balance"],
        "最大回撤" : R["report"]["max_drawdown"]*R["report"]["start_balance"]/(R["保证金"]*10*2)*100,
        "盈亏比" : R["report"]["profit_loss_ratio"],
        "胜率" : R["report"]["winning_rate"],
        "收益率" : (R["report"]["end_balance"]-R["report"]["start_balance"])/max(R["保证金"]*10*2,R["report"]["max_drawdown"]*R["report"]["start_balance"]*2)*100

    } 
    return pd.DataFrame(data,index=[0])

def create_params(symbol,strategy_name_list):
    #测试标的
    symbol,start_date,end_date = symbol["symbol"],symbol["start_date"],symbol["end_date"]
    y,m,d = start_date.split("-")
    y,m,d = int(y),int(m),int(d)
    e_y,e_m,e_d = end_date.split("-")
    e_y,e_m,e_d = int(e_y),int(e_m),int(e_d)
    #开始和结束时间
    start_date = date(y,m,d)
    end_date = date(e_y,e_m,e_d)
    #按照策略总数量，生成模拟账号
    acc_list = [TqSim(10000000) for _ in range(len(strategy_name_list))]
    #最大的长周期，用于确认订阅的K线长度
    long = max([int(strategy_name_list[i]["long"]) for i in range(len(strategy_name_list))])
    return symbol,start_date,end_date,acc_list,long

def run_backtest(symbol,strategy_name_list,user,result_dict_list):
    symbol,start_date,end_date,acc_list,long = create_params(symbol,strategy_name_list)
    api = TqApi(account=TqMultiAccount(acc_list),backtest=TqBacktest(start_dt=start_date, end_dt=end_date),auth=TqAuth(user["username"], user["password"]))
    quate = api.get_quote(symbol)
    margin = quate.margin
    klines = api.get_kline_serial(symbol, duration_seconds=60*60*24, data_length=long+3)
    target_pos_list = [TargetPosTask(api, symbol,account=acc_list[i]) for i in range(len(acc_list))]
    print("开始运行{}的回测".format(symbol))
    try:
        while True:
            api.wait_update()
            if api.is_changing(klines.iloc[-1], "datetime"):
                for i in range(len(strategy_name_list)):
                    craet_run_strategy(klines,strategy_name_list[i],target_pos_list[i])

    #strategy_name_list[0]= {'X_MA': 'MA', 'short': 5, 'long': 15}
    except BacktestFinished:
        print("回测结束")
        api.close()
        for r in range(len(acc_list)):
            result_dict = {"param_1":strategy_name_list[r]["X_MA"],
                                "param_2":str(strategy_name_list[r]["short"]),
                                "param_3":str(strategy_name_list[r]["long"]),
                                "标的":symbol,
                                "保证金" : margin,
                                "start_date":start_date,"end_date":end_date,"report":acc_list[r].tqsdk_stat}
            result_dict_list.append(create_backtest_record(result_dict))

class myThread (threading.Thread):
    def __init__(self, threadID, symbol, strategy_name_list,user,result_dict_list):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.symbol = symbol
        self.strategy_name_list = strategy_name_list
        self.user = user
        self.result_dict_list = result_dict_list
    def run(self):
        print ("开始线程：{}".format(self.threadID))
        run_backtest(self.symbol,self.strategy_name_list,self.user,self.result_dict_list)
        print ("退出线程：{}".format(self.threadID))




#第一部分：配置文件目录
symbols_pool_path = r"C:/Users/zhaoc/OneDrive/LH2024/HL2024-flask/策略研究/双均线策略/Symbols_and_Strategy/20250319标的池.json"
strategy_pool_path = r"C:/Users/zhaoc/OneDrive/LH2024/HL2024-flask/策略研究/双均线策略/Symbols_and_Strategy/策略池.json"
user_path = r"C:/Users/zhaoc/OneDrive/LH2024/HL2024-flask/策略研究/双均线策略/Symbols_and_Strategy/账户.json"
backtest_record_path = r"C:/Users/zhaoc/OneDrive/LH2024/HL2024-flask/策略研究/双均线策略/策略回测结果二{}.csv".format(date.today().strftime("%Y%m%d"))

# 第二部分:读取配置信息
with open(user_path,'r',encoding='utf8')as fp:
    json_data = json.load(fp)
    user = json_data['TQ_acc']
with open(symbols_pool_path,'r',encoding='utf8')as fp:
    json_data = json.load(fp)
    symbols = list(json_data['回测池——20250320'].values())
with open(strategy_pool_path,'r',encoding='utf8')as fp:
    json_data = json.load(fp)
    strategy_list = json_data['双均线策略-均线类型']
    param_list = json_data['双均线策略-均线周期']
    strategy_name_list = []
    for strategy in list(strategy_list.values()):
        for param in list(param_list.values()):
            strategy_name = {"X_MA": strategy,"short": param[0],"long": param[1]}
            strategy_name_list.append(strategy_name)

#第三部分多线程运行回测


start_backtest_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
print("开始回测时间:{}".format(start_backtest_time))

result_dict_list = []
thread_list = []

for i, symbol_target in enumerate(symbols[18:24]):
    thread = myThread(i, symbol_target, strategy_name_list,user,result_dict_list)  
    thread.start()
    thread_list.append(thread)
    time.sleep(15)

for t in thread_list:
    t.join()

print ("退出主线程")

#添加回测结果到文件下：
backtest_record_df = pd.concat(result_dict_list)
try:
    # 如果文件存在，追加数据
    existing_df = pd.read_csv(backtest_record_path)
    combined_df = pd.concat([existing_df, backtest_record_df], ignore_index=True)
    combined_df.to_csv(backtest_record_path, index=False)
except FileNotFoundError:
    # 如果文件不存在，直接保存数据
    backtest_record_df.to_csv(backtest_record_path, index=False)


end_backtest_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
print("开始回测时间:{}".format(start_backtest_time))
print("结束回测时间:{}".format(end_backtest_time))
